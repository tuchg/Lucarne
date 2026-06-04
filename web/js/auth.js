// Client-side auth for the terminal gateway (TASK-003 contract).
//
// The gateway's long-lived access token is a Bearer credential. Browsers can't
// set an Authorization header on a WebSocket and putting the token in the ws URL
// leaks it into logs, so ws connects use a two-step exchange:
//   1. POST /auth/ticket with `Authorization: Bearer <token>` → single-use ticket
//   2. open the ws with `?ticket=<t>` (the token NEVER goes in the ws URL)
// All /api/* calls carry `Authorization: Bearer <token>`.
//
// Token handling (minimize residual exposure):
//   - read from the URL fragment `#token=<key>` on load (fragment, not query, so
//     it never reaches server logs), stash in sessionStorage, then strip it from
//     the visible URL via history.replaceState.
//   - if absent, show a minimal login surface asking the user to paste the key.
//
// Backward-compat (local / no-auth mode): when the gateway runs with auth
// disabled, /auth/ticket still returns a ticket (the bearer guard is a no-op) so
// the flow is identical. If the server has no auth at all (404 on /auth/ticket)
// or signals auth-disabled, ws still connects tokenless and /api works without a
// header — local dev stays zero-friction.

const STORAGE_KEY = "lucarne.token";

// Once we learn the server enforces no auth, skip the ticket dance entirely.
let authDisabled = false;

/** The current access token from sessionStorage, or null. */
function getToken() {
  try {
    return sessionStorage.getItem(STORAGE_KEY);
  } catch (e) {
    return null;
  }
}

function setToken(token) {
  try {
    if (token) sessionStorage.setItem(STORAGE_KEY, token);
    else sessionStorage.removeItem(STORAGE_KEY);
  } catch (e) {}
}

/** Read `#token=<key>` from the URL fragment, store it, then scrub the hash so
 *  the credential doesn't linger in the visible URL / history. */
function readTokenFromUrl() {
  const hash = location.hash || "";
  const m = hash.match(/[#&]token=([^&]+)/);
  if (!m) return null;
  const token = decodeURIComponent(m[1]);
  setToken(token);
  // Strip the token from the visible URL without adding a history entry.
  const clean = location.pathname + location.search;
  try {
    history.replaceState(null, "", clean);
  } catch (e) {
    location.hash = "";
  }
  return token;
}

// Read the fragment token immediately at import time (before any logs/render).
readTokenFromUrl();

/** Headers for fetch() to /api/* — adds Bearer when a token is present. */
export function authHeaders(extra = {}) {
  const token = getToken();
  const headers = { ...extra };
  if (token) headers["Authorization"] = "Bearer " + token;
  return headers;
}

/** fetch() wrapper for /api/* that injects the Bearer header. On 401 it surfaces
 *  the login prompt (token missing/invalid) and the caller sees a failed
 *  response. Local no-auth mode works because the server ignores the header. */
export async function apiFetch(path, opts = {}) {
  const headers = authHeaders(opts.headers || {});
  const res = await fetch(path, { ...opts, headers });
  if (res.status === 401) {
    requireLogin("Access denied — paste your access key");
  }
  return res;
}

/** Exchange the access token for a single-use ws ticket.
 *
 *  Returns:
 *    - a ticket string when auth is enforced and the exchange succeeds
 *    - "" (empty) when the server signals auth is disabled or has no /auth/ticket
 *      route (404) — caller then connects tokenless (local backward-compat)
 *    - null when auth is required but we have no/invalid token (login surfaced)
 */
export async function wsTicket() {
  if (authDisabled) return "";
  let res;
  try {
    res = await fetch("/auth/ticket", {
      method: "POST",
      headers: authHeaders(),
    });
  } catch (e) {
    // Network/transport error — let the ws attempt proceed tokenless so the
    // existing reconnect loop can surface the failure.
    return "";
  }
  // No auth route at all (older/no-auth server) → tokenless ws is fine.
  if (res.status === 404) {
    authDisabled = true;
    return "";
  }
  if (res.status === 401) {
    requireLogin("Enter the gateway access key to connect");
    return null;
  }
  if (!res.ok) return "";
  let data = {};
  try {
    data = await res.json();
  } catch (e) {
    return "";
  }
  // A gateway with auth disabled still returns a ticket; either way `ticket`
  // (when present) is what we pass as ?ticket=.
  return typeof data.ticket === "string" ? data.ticket : "";
}

/** Append the ws ticket to a ws URL. Empty ticket (local no-auth) → unchanged. */
export function wsUrlWithTicket(baseUrl, ticket) {
  if (!ticket) return baseUrl;
  const sep = baseUrl.includes("?") ? "&" : "?";
  return `${baseUrl}${sep}ticket=${encodeURIComponent(ticket)}`;
}

// ---- minimal login surface ----

let loginOverlay = null;

/** True if we already hold a token. Used by views to decide whether to prompt
 *  up-front (a missing token only matters once the server says 401). */
export function hasToken() {
  return !!getToken();
}

/** Render a minimal, lightweight login overlay asking for the access key. When
 *  the user submits a key it is stored and `onSubmit` (e.g. a reconnect) runs.
 *  Idempotent: only one overlay at a time. */
export function requireLogin(message, onSubmit) {
  if (loginOverlay) {
    if (message) {
      const msg = loginOverlay.querySelector(".login-msg");
      if (msg) msg.textContent = message;
    }
    if (onSubmit) loginOverlay.__onSubmit = onSubmit;
    return;
  }

  const overlay = document.createElement("div");
  overlay.id = "login-overlay";
  overlay.__onSubmit = onSubmit;
  // Inline styles only (the app keeps all CSS in index.html; this is an
  // injected surface, so it carries its own minimal look matching the palette).
  Object.assign(overlay.style, {
    position: "fixed",
    inset: "0",
    background: "rgba(16,16,20,0.92)",
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
    zIndex: "9999",
    font: '13px "SF Mono", Menlo, Consolas, monospace',
    color: "#d8d8d8",
  });

  const card = document.createElement("div");
  Object.assign(card.style, {
    background: "#16161c",
    border: "1px solid #26262e",
    borderRadius: "8px",
    padding: "22px 24px",
    width: "340px",
    maxWidth: "90vw",
    boxShadow: "0 8px 30px rgba(0,0,0,0.5)",
  });

  const title = document.createElement("div");
  title.textContent = "Lucarne · access key";
  Object.assign(title.style, { fontWeight: "bold", marginBottom: "6px" });

  const msg = document.createElement("div");
  msg.className = "login-msg";
  msg.textContent = message || "Paste your gateway access key to connect.";
  Object.assign(msg.style, { color: "#8a8a96", fontSize: "12px", marginBottom: "12px" });

  const input = document.createElement("input");
  input.type = "password";
  input.placeholder = "access key";
  input.autocomplete = "off";
  input.spellcheck = false;
  Object.assign(input.style, {
    width: "100%",
    background: "#101014",
    color: "#d8d8d8",
    border: "1px solid #26262e",
    borderRadius: "6px",
    padding: "8px",
    font: "inherit",
    marginBottom: "12px",
  });

  const btn = document.createElement("button");
  btn.textContent = "Connect";
  Object.assign(btn.style, {
    background: "#4e9a06",
    color: "#fff",
    border: "0",
    borderRadius: "6px",
    padding: "7px 14px",
    cursor: "pointer",
    font: "inherit",
    width: "100%",
  });

  const submit = () => {
    const key = input.value.trim();
    if (!key) return;
    setToken(key);
    authDisabled = false;
    const cb = overlay.__onSubmit;
    closeLogin();
    if (cb) cb();
  };
  btn.onclick = submit;
  input.addEventListener("keydown", (e) => {
    if (e.key === "Enter") {
      e.preventDefault();
      submit();
    }
  });

  card.append(title, msg, input, btn);
  overlay.appendChild(card);
  document.body.appendChild(overlay);
  loginOverlay = overlay;
  input.focus();
}

function closeLogin() {
  if (loginOverlay && loginOverlay.parentNode) {
    loginOverlay.parentNode.removeChild(loginOverlay);
  }
  loginOverlay = null;
}
