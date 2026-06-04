// File tree panel (right side) — lazy-loads directories under the selected
// session's pane cwd. Files are draggable onto the terminal and right-clickable
// to insert their absolute path into the live pane.

import { apiFetch } from "./auth.js";

export function initFileTree({ onInsert } = {}) {
  const rootEl = document.getElementById("filetree");  const panel = document.getElementById("filetree-panel");
  const collapseBtn = document.getElementById("ft-collapse");
  let session = null;
  let cwd = "";

  collapseBtn.onclick = () => {
    panel.classList.toggle("collapsed");
    collapseBtn.textContent = panel.classList.contains("collapsed") ? "⮜" : "⮞";
  };

  async function fetchDir(path) {
    if (!session) return null;
    try {
      const url = `/api/sessions/${encodeURIComponent(session)}/files?path=${encodeURIComponent(path)}`;
      const r = await apiFetch(url);
      if (!r.ok) return null;
      return await r.json();
    } catch (e) {
      return null;
    }
  }

  function absPath(relPath) {
    const base = cwd.replace(/\/+$/, "");
    return relPath ? `${base}/${relPath}` : base;
  }

  function node(entry, parentPath, container) {
    const li = document.createElement("li");
    const row = document.createElement("div");
    row.className = "tnode" + (entry.dir ? "" : " file");
    const label = (open) => `${entry.dir ? (open ? "▾ 📁" : "▸ 📁") : "  📄"} ${entry.name}`;
    row.textContent = label(false);
    const path = parentPath ? `${parentPath}/${entry.name}` : entry.name;
    li.appendChild(row);

    if (entry.dir) {
      let childUl = null;
      let open = false;
      row.onclick = async () => {
        if (childUl) {
          open = !open;
          childUl.style.display = open ? "" : "none";
          row.textContent = label(open);
          return;
        }
        const data = await fetchDir(path);
        if (!data) return;
        childUl = document.createElement("ul");
        childUl.className = "tree";
        for (const e of data.entries) node(e, path, childUl);
        li.appendChild(childUl);
        open = true;
        row.textContent = label(true);
      };
    } else {
      // Files: drag onto terminal, or right-click to insert the absolute path.
      row.draggable = true;
      row.addEventListener("dragstart", (e) => {
        e.dataTransfer.setData("text/plain", absPath(path));
      });
      row.oncontextmenu = (e) => {
        e.preventDefault();
        if (onInsert) onInsert(absPath(path));
      };
      row.title = "drag to terminal, or right-click to insert path";
    }
    container.appendChild(li);
  }

  async function load(sessionId) {
    session = sessionId;
    rootEl.innerHTML = "";
    const data = await fetchDir("");
    if (!data) { rootEl.textContent = "(no cwd for this session)"; return; }
    cwd = data.cwd || "";
    const ul = document.createElement("ul");
    ul.className = "tree";
    for (const e of data.entries) node(e, "", ul);
    rootEl.appendChild(ul);
  }

  return { load };
}
