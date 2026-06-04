// Terminal view — rmux mirror over /ws, with zoom controls, a cwd-aware file
// tree drop target, and the agent/archive affordances. Exposes insertText() so
// the file tree can write a path into the live pane.
//
// Auth (TASK-003 contract, via ./auth.js): the access token is read from the
// `#token=` URL fragment (then the hash is cleared via history.replaceState),
// stored in sessionStorage, and presented as `Authorization: Bearer <token>` on
// every /api/* call (apiFetch). The ws never carries the token: connect() first
// POSTs to /auth/ticket for a single-use ticket and opens /ws?ticket=<t>.

import { CanvasRenderer } from "./renderer.js";
import { apiFetch, wsTicket, wsUrlWithTicket, requireLogin } from "./auth.js";

const esc = (s) =>
  s.replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));

export function initTerminal({ onAgent, onSelect } = {}) {
  const canvas = document.getElementById("screen");
  const wrap = document.getElementById("screenwrap");
  const renderer = new CanvasRenderer(canvas);
  const sessionsEl = document.getElementById("sessions");
  const statusEl = document.getElementById("term-status");
  const connEl = document.getElementById("conn");

  let ws = null;
  let current = null;
  let statusText = "connecting…";
  let agentInfo = null;

  const setConn = (t) => { if (connEl) connEl.textContent = t; };
  const send = (f) => { if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(f)); };

  function renderStatus() {
    statusEl.textContent = statusText;
    if (agentInfo && agentInfo.bound && onAgent) {
      const b = document.createElement("button");
      b.className = "agentbtn";
      b.textContent = `🤖 ${agentInfo.kind} · open chat`;
      b.onclick = () => onAgent(current);
      statusEl.appendChild(b);
    }
    if (current) {
      const ab = document.createElement("button");
      ab.className = "agentbtn";
      ab.textContent = "🗄 archive";
      ab.onclick = archiveCurrent;
      statusEl.appendChild(ab);
    }
  }
  const setStatus = (t) => { statusText = t; renderStatus(); };

  async function archiveCurrent() {
    if (!current) return;
    if (!confirm("Archive (close) this terminal? Its content is preserved.")) return;
    try { await apiFetch(`/api/sessions/${encodeURIComponent(current)}/archive`, { method: "POST" }); } catch (e) {}
    current = null; agentInfo = null;
    setStatus("archived — terminal closed, content preserved");
    refreshSessions();
  }
  async function refreshSessions() {
    try { renderSessions(await (await apiFetch("/api/sessions")).json()); } catch (e) {}
  }

  async function connect() {
    const proto = location.protocol === "https:" ? "wss" : "ws";
    // Exchange the long-lived token for a single-use ws ticket. null = auth
    // required but no/invalid token (login surfaced) — retry once submitted.
    const ticket = await wsTicket();
    if (ticket === null) {
      setStatus("waiting for access key…"); setConn("○ login required");
      requireLogin("Enter the gateway access key to connect", connect);
      return;
    }
    const url = wsUrlWithTicket(`${proto}://${location.host}/ws`, ticket);
    ws = new WebSocket(url);
    ws.onopen = () => { setStatus("connected"); setConn("● connected"); };
    ws.onmessage = (ev) => handle(JSON.parse(ev.data));
    ws.onerror = () => setStatus("ws error");
    ws.onclose = () => { setStatus("disconnected — reconnecting…"); setConn("○ reconnecting…"); setTimeout(connect, 1500); };
  }

  function handle(f) {
    switch (f.type) {
      case "session_list": renderSessions(f.sessions); break;
      case "snapshot": if (f.session === current) renderer.renderSnapshot(f.grid, f.cursor); break;
      case "snapshot_delta": if (f.session === current) renderer.renderDelta(f.delta, f.rev, f.cursor); break;
      case "session_created": current = f.session; break;
      case "session_closed": if (f.session === current) current = null; break;
      case "error": setStatus("error: " + f.msg); break;
    }
  }

  function renderSessions(sessions) {
    sessionsEl.innerHTML = "";
    for (const s of sessions) {
      const li = document.createElement("li");
      const badge = s.origin === "managed" ? "M" : "A";
      li.innerHTML =
        `${esc(s.title)} <span class="badge">[${badge}]</span>` +
        (s.cwd ? `<span class="cwd">${esc(s.cwd)}</span>` : "");
      if (s.id === current) li.className = "active";
      li.onclick = () => subscribe(s.id, li);
      sessionsEl.appendChild(li);
    }
  }

  async function subscribe(id, li) {
    current = id;
    agentInfo = null;
    send({ type: "subscribe", session: id });
    [...sessionsEl.children].forEach((el) => el.classList.remove("active"));
    if (li) li.classList.add("active");
    canvas.focus();
    setStatus("session " + id);
    if (onSelect) onSelect(id);
    try {
      const a = await (await apiFetch(`/api/sessions/${encodeURIComponent(id)}/agent`)).json();
      if (current === id) { agentInfo = a; renderStatus(); }
    } catch (e) {}
  }

  /** Write text into the current pane (used by file-tree drag/right-click). */
  function insertText(text) {
    if (!current || !text) return;
    send({ type: "input", session: current, event: { kind: "text", text } });
    setStatus("inserted: " + text);
  }

  // ---- zoom ----
  document.getElementById("zoom-in").onclick = () => renderer.setZoom(renderer.zoom + 0.1);
  document.getElementById("zoom-out").onclick = () => renderer.setZoom(renderer.zoom - 0.1);
  document.getElementById("zoom-fit").onclick = () => {
    const w = canvas.offsetWidth;
    if (w > 0) renderer.setZoom(renderer.zoom * ((wrap.clientWidth - 24) / w));
  };

  // ---- file drop → insert path ----
  wrap.addEventListener("dragover", (e) => { e.preventDefault(); });
  wrap.addEventListener("drop", (e) => {
    e.preventDefault();
    const path = e.dataTransfer.getData("text/plain");
    if (path) insertText(path);
  });

  // ---- keyboard ----
  const NAMED = {
    Enter: "enter", Tab: "tab", Backspace: "backspace", Escape: "escape",
    ArrowUp: "up", ArrowDown: "down", ArrowLeft: "left", ArrowRight: "right",
    Home: "home", End: "end", PageUp: "page_up", PageDown: "page_down", Delete: "delete",
  };
  function mods(e) {
    return { ctrl: e.ctrlKey, alt: e.altKey, shift: e.shiftKey, meta: e.metaKey };
  }
  function keyToInput(e) {
    const key = e.key;
    if (e.ctrlKey && key.length === 1 && /[a-zA-Z]/.test(key)) {
      return { kind: "control", key: { ctrl_char: key.toLowerCase() } };
    }
    if (e.altKey || e.metaKey || (e.shiftKey && NAMED[key])) {
      return { kind: "key", code: key, mods: mods(e) };
    }
    if (NAMED[key]) return { kind: "control", key: NAMED[key] };
    if (key.length === 1 && !e.metaKey) return { kind: "text", text: key };
    return null;
  }
  canvas.addEventListener("keydown", (e) => {
    if (!current) return;
    const event = keyToInput(e);
    if (event) { e.preventDefault(); send({ type: "input", session: current, event }); }
  });

  document.getElementById("new-session").onclick = () => {
    const title = prompt("New session title:", "shell");
    if (title !== null) send({ type: "create_session", title: title || null });
  };

  connect();
  return { insertText };
}
