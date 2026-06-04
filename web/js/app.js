// Dual-mode web shell. One converter, multiple surfaces:
//   Terminal (rmux mirror /ws) + cwd file tree (drag/right-click → pane)
//   Chat (managed /chat, bound /agent/<id>, or read-only history)
// All connect to `location.host`, so local (direct) and remote (cloudflared
// tunnel) are byte-identical to this code.

import { initTerminal } from "./terminal.js";
import { initChat } from "./chat.js";
import { initFileTree } from "./filetree.js";

const tabTerminal = document.getElementById("tab-terminal");
const tabChat = document.getElementById("tab-chat");
const termView = document.getElementById("terminal-view");
const chatView = document.getElementById("chat-view");

const chat = initChat();

function show(mode) {
  const term = mode === "terminal";
  tabTerminal.classList.toggle("active", term);
  tabChat.classList.toggle("active", !term);
  termView.classList.toggle("active", term);
  chatView.classList.toggle("active", !term);
  if (term) document.getElementById("screen").focus();
  else { chat.loadPickers(); document.getElementById("chat-input").focus(); }
}

tabTerminal.onclick = () => show("terminal");
tabChat.onclick = () => show("chat");

const fileTree = initFileTree({
  onInsert: (path) => terminal.insertText(path),
});

const terminal = initTerminal({
  // A pane with a bound agent → open its transcript in the chat tab.
  onAgent: (sessionId) => {
    chat.connectTo(`/agent/${encodeURIComponent(sessionId)}`);
    show("chat");
  },
  // Selecting a session loads its cwd file tree.
  onSelect: (sessionId) => fileTree.load(sessionId),
});
