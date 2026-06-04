// Chat view — one converter, three sources via a picker (each labelled with a
// human name = the session's first user message). Assistant output is rendered
// as markdown.
//   ➕ New agent        → /chat  (fresh managed agent)
//   bound: <name>       → /agent/<term-id>  (drives the live pane)
//   history: <name>     → read-only transcript replay
//
// Auth (TASK-003 contract, via ./auth.js): /api/* calls carry
// `Authorization: Bearer <token>` (apiFetch). The ws never carries the token —
// connect() first POSTs to /auth/ticket for a single-use ticket, then opens the
// chat/agent ws with `?ticket=<t>`.

import { renderMarkdown } from "./markdown.js";
import { apiFetch, wsTicket, wsUrlWithTicket, requireLogin } from "./auth.js";

export function initChat() {
  const logEl = document.getElementById("chat-log");
  const inputEl = document.getElementById("chat-input");
  const sendBtn = document.getElementById("chat-send");
  const sourceEl = document.getElementById("chat-source");
  const newBtn = document.getElementById("chat-new");
  const picker = document.getElementById("chat-picker");

  let ws = null;
  let streamBubble = null;
  let path = "/chat";
  let readonly = false;

  const setSource = (t) => { if (sourceEl) sourceEl.textContent = t; };

  function bubble(cls, text, md) {
    const d = document.createElement("div");
    d.className = `bubble ${cls}`;
    if (md) d.innerHTML = renderMarkdown(text);
    else d.textContent = text;
    logEl.appendChild(d);
    logEl.scrollTop = logEl.scrollHeight;
    return d;
  }

  function closeWs() {
    if (ws) { try { ws.onclose = null; ws.close(); } catch (e) {} ws = null; }
  }

  async function connect() {
    closeWs();
    const proto = location.protocol === "https:" ? "wss" : "ws";
    const connectPath = path;
    // Exchange the long-lived token for a single-use ws ticket before opening
    // the chat/agent ws. The token NEVER goes in the ws URL — only ?ticket=.
    const ticket = await wsTicket();
    if (ticket === null) {
      requireLogin("Enter the gateway access key to connect", connect);
      return;
    }
    // The source may have switched while awaiting the ticket — bail if stale.
    if (connectPath !== path) return;
    const url = wsUrlWithTicket(`${proto}://${location.host}${path}`, ticket);
    ws = new WebSocket(url);
    ws.onmessage = (ev) => handle(JSON.parse(ev.data));
    ws.onclose = () => { streamBubble = null; if (ws && ws.__path === path && !readonly) setTimeout(connect, 2000); };
    ws.onerror = () => {};
    ws.__path = path;
  }

  /** Switch to a live source (/chat or /agent/<id>), with a display name. */
  function connectTo(nextPath, name) {
    readonly = false;
    inputEl.disabled = false;
    path = nextPath;
    logEl.innerHTML = "";
    streamBubble = null;
    setSource(name || (nextPath === "/chat" ? "managed agent (new session)" : "bound · " + decodeURIComponent(nextPath.replace("/agent/", ""))));
    connect();
  }

  /** Show a read-only history transcript (no live pane to drive). */
  async function showHistory(session, name) {
    readonly = true;
    inputEl.disabled = true;
    closeWs();
    logEl.innerHTML = "";
    streamBubble = null;
    setSource("history (read-only) · " + (name || session.slice(0, 8)));
    try {
      const o = await (await apiFetch(`/api/agent-history/${encodeURIComponent(session)}`)).json();
      for (const m of o.messages || []) bubble(m.role === "user" ? "user" : "assistant", m.text, m.role !== "user");
      if (!(o.messages || []).length) bubble("meta", "(empty transcript)");
    } catch (e) {
      bubble("meta", "failed to load history");
    }
  }

  function handle(f) {
    switch (f.type) {
      case "ready":
        bubble("meta", "agent ready" + (f.provider ? " · " + f.provider : "") + (f.session_id ? " · " + f.session_id.slice(0, 8) : ""));
        break;
      case "message":
        if (f.role === "user") {
          bubble("user", f.text);
        } else if (f.streaming) {
          if (!streamBubble) { streamBubble = bubble("assistant", "", true); streamBubble.__raw = ""; }
          streamBubble.__raw += f.text;
          streamBubble.innerHTML = renderMarkdown(streamBubble.__raw);
          logEl.scrollTop = logEl.scrollHeight;
        } else {
          if (streamBubble) streamBubble = null;
          else bubble("assistant", f.text, true);
        }
        break;
      case "reasoning": bubble("reasoning", f.text); break;
      case "tool": bubble("meta", "⚙ " + (f.name || "tool")); break;
      case "turn_complete": streamBubble = null; break;
      case "turn_failed": bubble("meta", "turn failed: " + (f.error || "")); streamBubble = null; break;
      case "approval": renderApproval(f); break;
      case "error": bubble("meta", "error: " + (f.msg || "")); break;
    }
  }

  function renderApproval(f) {
    const d = bubble("assistant", "🔐 approval requested: " + (f.tool || "") + (f.message ? "\n" + f.message : ""));
    const row = document.createElement("div");
    row.className = "approve";
    const allow = document.createElement("button");
    allow.className = "allow"; allow.textContent = "Approve";
    const deny = document.createElement("button");
    deny.className = "deny"; deny.textContent = "Deny";
    allow.onclick = () => { send({ type: "approve", req_id: f.req_id, allow: true }); row.remove(); };
    deny.onclick = () => { send({ type: "approve", req_id: f.req_id, allow: false }); row.remove(); };
    row.append(allow, deny);
    d.appendChild(row);
  }

  function send(f) { if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(f)); }

  function submit() {
    if (readonly) { bubble("meta", "this is a read-only history view"); return; }
    const text = inputEl.value.trim();
    if (!text) return;
    bubble("user", text);
    send({ type: "prompt", text });
    inputEl.value = "";
    streamBubble = null;
  }

  sendBtn.onclick = submit;
  inputEl.addEventListener("keydown", (e) => {
    if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); submit(); }
  });
  newBtn.onclick = () => { picker.value = ""; connectTo("/chat"); };

  // Picker: bound terminal agents + recent rmux-related history, each named by
  // its first user message so they're easy to tell apart.
  async function loadPickers() {
    let opts = '<option value="">— bound agents / history —</option>';
    try {
      const a = await (await apiFetch("/api/agents")).json();
      if ((a.agents || []).length) {
        opts += '<optgroup label="bound terminal agents">';
        for (const g of a.agents) {
          const name = g.summary ? `“${g.summary}”` : g.agent_session_id.slice(0, 8);
          opts += `<option value="agent:${encodeURIComponent(g.term_session)}">🖥 ${g.title} · ${name}</option>`;
        }
        opts += "</optgroup>";
      }
    } catch (e) {}
    try {
      const h = await (await apiFetch("/api/agent-history")).json();
      if ((h.sessions || []).length) {
        opts += '<optgroup label="rmux agent history (read-only)">';
        for (const g of h.sessions.slice(0, 20)) {
          const name = g.summary ? `“${g.summary}”` : g.session_id.slice(0, 8);
          const where = g.title || g.rmux_session || "";
          opts += `<option value="hist:${encodeURIComponent(g.session_id)}">🕘 ${where ? where + " · " : ""}${name}</option>`;
        }
        opts += "</optgroup>";
      }
    } catch (e) {}
    picker.innerHTML = opts;
  }

  picker.onchange = () => {
    const v = picker.value;
    if (!v) return;
    const name = picker.options[picker.selectedIndex] ? picker.options[picker.selectedIndex].text : "";
    if (v.startsWith("agent:")) {
      connectTo(`/agent/${v.slice("agent:".length)}`, name);
    } else if (v.startsWith("hist:")) {
      showHistory(decodeURIComponent(v.slice("hist:".length)), name);
    }
  };

  setSource("managed agent (new session)");
  connect();
  return { connectTo, loadPickers };
}
