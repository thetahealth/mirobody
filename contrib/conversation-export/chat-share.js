/**
 * Conversation export for Mirobody — the button, the picker, the two formats.
 *
 * Adds itself to the bundled client from outside, because `frontend/` is build
 * output: the client's source lives elsewhere, and anything written into that
 * directory is deleted by the next `rsync --delete`. So this hooks in as an
 * add-on instead — a browser extension loads it (see `content.js`), or your own
 * build can `<script type="module">` it.
 *
 * It touches two things only: the documented HTTP surface
 * (`GET /api/history?session_id=`) and the DOM by class-name *fragment*
 * (`_header_`, `_right_`, `_title_`), which is what survives the per-build hash
 * suffix. Nothing here reads the bundle.
 *
 * Mounting is idempotent and driven by a timer, because the client is a SPA:
 * the header is re-created on every navigation and the session id lives in the
 * path.
 */

import { renderConversationToPng } from "./chat-share-image.js";

const isZh = /^zh/i.test(navigator.language || "");

const T = isZh
  ? {
      action: "分享",
      title: "分享这段对话",
      hint: "勾选要导出的消息，再选一种格式。",
      selectAll: "全选",
      clear: "清空",
      onlyMine: "只看我的",
      onlyAi: "只看回答",
      mine: "我",
      ai: "Mirobody",
      format: "格式",
      markdown: "Markdown",
      markdownNote: "纯文本，方便贴进笔记",
      image: "图片",
      imageNote: "PNG 长图，方便发给别人",
      cancel: "取消",
      export: (n) => `导出 ${n} 条`,
      picked: (n, total) => `已选 ${n} / ${total} 条`,
      empty: "这段对话还没有内容",
      nothing: "请至少选择一条消息",
      loading: "正在读取对话…",
      failed: "读取对话失败，请重试",
      working: "正在生成…",
      tooLong: "内容太长，一张图放不下，少选几条试试",
      done: "已导出",
      attachment: "附件",
      exported: "导出自 Mirobody",
      time: (iso) =>
        new Date(iso).toLocaleString("zh-CN", {
          month: "numeric",
          day: "numeric",
          hour: "2-digit",
          minute: "2-digit",
        }),
    }
  : {
      action: "Share",
      title: "Share this conversation",
      hint: "Pick the messages to include, then a format.",
      selectAll: "All",
      clear: "None",
      onlyMine: "Mine only",
      onlyAi: "Answers only",
      mine: "Me",
      ai: "Mirobody",
      format: "Format",
      markdown: "Markdown",
      markdownNote: "Plain text, easy to paste into notes",
      image: "Image",
      imageNote: "PNG poster, easy to send to someone",
      cancel: "Cancel",
      export: (n) => `Export ${n}`,
      picked: (n, total) => `${n} of ${total} selected`,
      empty: "Nothing in this conversation yet",
      nothing: "Select at least one message",
      loading: "Reading the conversation…",
      failed: "Could not read the conversation",
      working: "Rendering…",
      tooLong: "Too long for a single image — select fewer messages",
      done: "Exported",
      attachment: "Attachment",
      exported: "Exported from Mirobody",
      time: (iso) =>
        new Date(iso).toLocaleString(undefined, {
          month: "short",
          day: "numeric",
          hour: "2-digit",
          minute: "2-digit",
        }),
    };

const CSS = `
.mcs-btn{display:inline-flex;align-items:center;gap:6px;height:32px;padding:0 11px;
  flex:0 0 auto;border-radius:9px;border:1px solid var(--color-border,#e8e4dc);background:transparent;
  color:var(--color-text-primary,#1a1a1a);font-weight:500;font-size:13px;line-height:1;
  cursor:pointer;
  transition:background .15s ease,border-color .15s ease;white-space:nowrap}
.mcs-btn:hover{background:var(--color-bg-soft,#f1efe8)}
.mcs-btn:active{transform:translateY(.5px)}
.mcs-btn svg{flex:none}

.mcs-mask{position:fixed;inset:0;z-index:2147483000;display:flex;align-items:center;
  justify-content:center;padding:24px;background:rgba(24,21,18,.42);
  backdrop-filter:blur(2px);animation:mcs-fade .16s ease-out}
@keyframes mcs-fade{from{opacity:0}to{opacity:1}}
.mcs-panel{width:min(700px,100%);max-height:min(780px,88vh);display:flex;flex-direction:column;
  background:var(--color-bg,#fff);color:var(--color-text-primary,#1a1a1a);border-radius:16px;
  box-shadow:0 24px 70px rgba(20,16,12,.28);overflow:hidden;
  animation:mcs-rise .18s ease-out;font-size:14px}
@keyframes mcs-rise{from{transform:translateY(10px);opacity:0}to{transform:none;opacity:1}}

.mcs-head{display:flex;align-items:flex-start;gap:12px;padding:18px 20px 14px}
.mcs-head h2{margin:0;font-size:16px;font-weight:650;letter-spacing:-.01em}
.mcs-head p{margin:4px 0 0;font-size:12.5px;color:var(--color-text-secondary,#5c5a57)}
.mcs-x{margin-left:auto;width:30px;height:30px;border:0;border-radius:8px;background:transparent;
  color:var(--color-text-secondary,#5c5a57);cursor:pointer;display:grid;place-items:center}
.mcs-x:hover{background:var(--color-bg-soft,#f1efe8)}

.mcs-tools{display:flex;flex-wrap:wrap;gap:8px;padding:0 20px 12px}
.mcs-chip{height:28px;padding:0 10px;border-radius:8px;border:1px solid var(--color-border,#e8e4dc);
  background:transparent;color:var(--color-text-secondary,#5c5a57);font-size:12.5px;cursor:pointer}
.mcs-chip:hover{background:var(--color-bg-soft,#f1efe8);color:var(--color-text-primary,#1a1a1a)}

.mcs-list{flex:1;min-height:120px;overflow:auto;padding:0 14px 8px;
  border-top:1px solid var(--color-border,#e8e4dc);
  border-bottom:1px solid var(--color-border,#e8e4dc)}
.mcs-item{display:flex;gap:10px;padding:11px 8px;border-radius:10px;cursor:pointer;
  align-items:flex-start}
.mcs-item:hover{background:var(--color-bg-soft,#f1efe8)}
.mcs-item input{margin:3px 0 0;accent-color:var(--color-accent,#0e3880);width:15px;height:15px;
  cursor:pointer;flex:none}
.mcs-body{min-width:0;flex:1}
.mcs-meta{display:flex;align-items:center;gap:8px;margin-bottom:3px}
.mcs-role{font-size:11.5px;font-weight:600;padding:1px 7px;border-radius:6px;
  background:rgba(14,56,128,.09);color:var(--color-accent,#0e3880)}
.mcs-role.is-user{background:var(--color-bg-soft,#f1efe8);color:var(--color-text-secondary,#5c5a57)}
.mcs-time{font-size:11.5px;color:var(--color-text-secondary,#5c5a57);opacity:.75}
.mcs-text{font-size:13px;line-height:1.6;color:var(--color-text-primary,#1a1a1a);
  display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden;
  word-break:break-word}
.mcs-empty,.mcs-error{padding:28px 20px;text-align:center;font-size:13px;
  color:var(--color-text-secondary,#5c5a57)}

.mcs-foot{padding:14px 20px 18px;display:flex;flex-direction:column;gap:12px}
.mcs-formats{display:flex;gap:10px}
.mcs-fmt{flex:1;text-align:left;padding:10px 12px;border-radius:11px;cursor:pointer;
  border:1px solid var(--color-border,#e8e4dc);background:transparent}
.mcs-fmt[aria-checked="true"]{border-color:var(--color-accent,#0e3880);
  box-shadow:inset 0 0 0 1px var(--color-accent,#0e3880)}
.mcs-fmt b{display:block;font-size:13px;font-weight:600;color:var(--color-text-primary,#1a1a1a)}
.mcs-fmt span{display:block;font-size:11.5px;margin-top:2px;
  color:var(--color-text-secondary,#5c5a57)}
.mcs-actions{display:flex;align-items:center;gap:10px}
.mcs-count{font-size:12.5px;color:var(--color-text-secondary,#5c5a57)}
.mcs-actions .spacer{flex:1}
.mcs-primary{height:36px;padding:0 18px;border:0;border-radius:10px;cursor:pointer;
  /* accent flips light in dark mode, so the label takes the page colour:
     dark ink on a light accent, paper on a deep one. */
  background:var(--color-accent,#0e3880);color:var(--color-bg,#fff);
  font-size:13.5px;font-weight:600}
.mcs-primary:disabled{opacity:.45;cursor:default}
.mcs-ghost{height:36px;padding:0 14px;border-radius:10px;cursor:pointer;background:transparent;
  border:1px solid var(--color-border,#e8e4dc);color:var(--color-text-primary,#1a1a1a);font-size:13px}
.mcs-ghost:hover{background:var(--color-bg-soft,#f1efe8)}
@media (max-width:640px){
  .mcs-mask{padding:0;align-items:flex-end}
  .mcs-panel{width:100%;max-height:92vh;border-radius:16px 16px 0 0}
  .mcs-formats{flex-direction:column}
}
`;

// ── helpers ────────────────────────────────────────────────────────────────

function injectStyle() {
  if (document.getElementById("mcs-style")) return;
  const style = document.createElement("style");
  style.id = "mcs-style";
  style.textContent = CSS;
  document.head.appendChild(style);
}

/** `/ask/<id>` and `/chat/<id>` are the two routes that carry a session. */
export function sessionId() {
  const match = location.pathname.match(/^\/(?:ask|chat)\/([^/?#]+)/);
  return match ? decodeURIComponent(match[1]) : "";
}

function headerRight() {
  const anchor = document.getElementById("chat_vs_model");
  const header = anchor
    ? anchor.closest('[class*="_header_"]')
    : document.querySelector('[class*="_header_"]');
  return header ? header.querySelector('[class*="_right_"]') : null;
}

function conversationTitle() {
  const title = document.querySelector('[class*="_title_"]');
  const text = title ? title.textContent.trim() : "";
  return text && text.length < 80 ? text : T.ai;
}

function token() {
  return localStorage.getItem("ACCESS_TOKEN") || "";
}

function plainPreview(text) {
  return String(text || "")
    .replace(/```[\s\S]*?```/g, " ")
    .replace(/[#>*_`|-]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function safeName(name) {
  return (name || "mirobody-chat")
    .replace(/[\\/:*?"<>|\n\r\t]/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 60);
}

function stamp() {
  const d = new Date();
  const p = (n) => String(n).padStart(2, "0");
  return `${d.getFullYear()}${p(d.getMonth() + 1)}${p(d.getDate())}-${p(d.getHours())}${p(d.getMinutes())}`;
}

function download(blob, filename) {
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = filename;
  link.rel = "noopener";
  document.body.appendChild(link);
  link.click();
  // The anchor and its blob URL are kept alive for a while on purpose:
  // Chromium aborts a download whose object URL is revoked before it has been
  // read, and revoking right after `click()` is exactly that race.
  setTimeout(() => {
    link.remove();
    URL.revokeObjectURL(url);
  }, 60000);
}

/** Normalise `/api/history` rows into what the picker and the poster need. */
function toEntries(history) {
  return history
    .map((row, index) => {
      const role = row.role === "user" ? "user" : "assistant";
      let text = typeof row.content === "string" ? row.content : "";
      const dict = row.content_dict;
      if (Array.isArray(dict)) {
        const files = dict.flatMap((block) => (block && block.files) || []);
        if (files.length) {
          const names = files.map((f) => f.file_name || f.filename || f.original_filename || "file");
          text = `${text}\n\n[${T.attachment}] ${names.join(", ")}`.trim();
        }
      }
      return {
        id: row.id || `m${index}`,
        role,
        label: role === "user" ? T.mine : T.ai,
        time: row.timestamp ? T.time(row.timestamp) : "",
        iso: row.timestamp || "",
        text,
        preview: plainPreview(text).slice(0, 90) || `[${T.attachment}]`,
      };
    })
    .filter((entry) => entry.text.trim());
}

// ── markdown export ────────────────────────────────────────────────────────

export function toMarkdown(title, entries) {
  const when = new Date().toLocaleString(isZh ? "zh-CN" : undefined);
  const lines = [`# ${title}`, "", `> ${T.exported} · ${when}`, ""];
  entries.forEach((entry, index) => {
    if (index > 0) lines.push("---", "");
    lines.push(`## ${entry.label}${entry.iso ? ` · ${entry.time}` : ""}`, "", entry.text.trim(), "");
  });
  return lines.join("\n");
}

// ── panel ──────────────────────────────────────────────────────────────────

let state = null; // { sid, entries, picked:Set, format:'md'|'png', busy }

function closePanel() {
  const mask = document.getElementById("mcs-mask");
  if (mask) mask.remove();
  state = null;
  document.removeEventListener("keydown", onKey);
}

function onKey(event) {
  if (event.key === "Escape") {
    event.stopPropagation();
    closePanel();
  }
}

function renderList() {
  const list = document.querySelector(".mcs-list");
  if (!list || !state) return;
  if (!state.entries.length) {
    list.innerHTML = `<div class="mcs-empty">${T.empty}</div>`;
    return;
  }
  list.innerHTML = state.entries
    .map(
      (entry) => `
      <label class="mcs-item" data-id="${entry.id}">
        <input type="checkbox" ${state.picked.has(entry.id) ? "checked" : ""}>
        <div class="mcs-body">
          <div class="mcs-meta">
            <span class="mcs-role ${entry.role === "user" ? "is-user" : ""}">${entry.label}</span>
            <span class="mcs-time">${entry.time}</span>
          </div>
          <div class="mcs-text">${escapeHtml(entry.preview)}</div>
        </div>
      </label>`
    )
    .join("");

  list.querySelectorAll(".mcs-item").forEach((item) => {
    const id = item.dataset.id;
    const box = item.querySelector("input");
    box.addEventListener("change", () => {
      if (box.checked) state.picked.add(id);
      else state.picked.delete(id);
      syncCount();
    });
    item.addEventListener("click", (event) => {
      if (event.target !== box) {
        event.preventDefault();
        box.checked = !box.checked;
        box.dispatchEvent(new Event("change"));
      }
    });
  });
  syncCount();
}

function escapeHtml(text) {
  return String(text).replace(/[&<>"]/g, (ch) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[ch]));
}

function syncCount() {
  const button = document.querySelector(".mcs-primary");
  const count = document.querySelector(".mcs-count");
  if (!button || !state) return;
  const n = state.picked.size;
  button.textContent = state.busy ? T.working : T.export(n);
  button.disabled = n === 0 || state.busy;
  if (count) count.textContent = T.picked(n, state.entries.length);
  const allBtn = document.querySelector('[data-act="all"]');
  if (allBtn) allBtn.textContent = T.selectAll;
}

function openPanel() {
  const sid = sessionId();
  if (!sid || document.getElementById("mcs-mask")) return;

  injectStyle();
  state = { sid, entries: [], picked: new Set(), format: "md", busy: false };

  const mask = document.createElement("div");
  mask.className = "mcs-mask";
  mask.id = "mcs-mask";
  mask.innerHTML = `
    <div class="mcs-panel" role="dialog" aria-modal="true">
      <div class="mcs-head">
        <div>
          <h2>${T.title}</h2>
          <p>${T.hint}</p>
        </div>
        <button class="mcs-x" aria-label="${T.cancel}">
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor"
            stroke-width="2" stroke-linecap="round"><path d="M18 6L6 18M6 6l12 12"/></svg>
        </button>
      </div>
      <div class="mcs-tools">
        <button class="mcs-chip" data-act="all">${T.selectAll}</button>
        <button class="mcs-chip" data-act="none">${T.clear}</button>
        <button class="mcs-chip" data-act="mine">${T.onlyMine}</button>
        <button class="mcs-chip" data-act="ai">${T.onlyAi}</button>
      </div>
      <div class="mcs-list"><div class="mcs-empty">${T.loading}</div></div>
      <div class="mcs-foot">
        <div class="mcs-formats" role="radiogroup" aria-label="${T.format}">
          <button class="mcs-fmt" role="radio" aria-checked="true" data-fmt="md">
            <b>${T.markdown}</b><span>${T.markdownNote}</span>
          </button>
          <button class="mcs-fmt" role="radio" aria-checked="false" data-fmt="png">
            <b>${T.image}</b><span>${T.imageNote}</span>
          </button>
        </div>
        <div class="mcs-actions">
          <span class="mcs-count"></span>
          <span class="spacer"></span>
          <button class="mcs-ghost" data-act="cancel">${T.cancel}</button>
          <button class="mcs-primary" disabled>${T.export(0)}</button>
        </div>
      </div>
    </div>`;

  document.body.appendChild(mask);
  document.addEventListener("keydown", onKey);

  mask.addEventListener("click", (event) => {
    if (event.target === mask) closePanel();
  });
  mask.querySelector(".mcs-x").addEventListener("click", closePanel);
  mask.querySelector('[data-act="cancel"]').addEventListener("click", closePanel);

  mask.querySelectorAll(".mcs-chip").forEach((chip) => {
    chip.addEventListener("click", () => {
      if (!state) return;
      const act = chip.dataset.act;
      if (act === "all") state.picked = new Set(state.entries.map((e) => e.id));
      if (act === "none") state.picked = new Set();
      if (act === "mine")
        state.picked = new Set(state.entries.filter((e) => e.role === "user").map((e) => e.id));
      if (act === "ai")
        state.picked = new Set(state.entries.filter((e) => e.role === "assistant").map((e) => e.id));
      renderList();
    });
  });

  mask.querySelectorAll(".mcs-fmt").forEach((button) => {
    button.addEventListener("click", () => {
      if (!state) return;
      state.format = button.dataset.fmt;
      mask.querySelectorAll(".mcs-fmt").forEach((other) => {
        other.setAttribute("aria-checked", String(other === button));
      });
    });
  });

  mask.querySelector(".mcs-primary").addEventListener("click", runExport);

  loadEntries(sid);
}

async function loadEntries(sid) {
  const list = document.querySelector(".mcs-list");
  try {
    const response = await fetch(`/api/history?session_id=${encodeURIComponent(sid)}`, {
      headers: { Authorization: `Bearer ${token()}` },
    });
    const payload = await response.json();
    const history = (payload && payload.data && payload.data.history) || [];
    if (!state || state.sid !== sid) return;
    state.entries = toEntries(history);
    state.picked = new Set(state.entries.map((entry) => entry.id));
    renderList();
  } catch {
    if (list) list.innerHTML = `<div class="mcs-error">${T.failed}</div>`;
  }
}

async function runExport() {
  if (!state || state.busy || !state.picked.size) return;
  const entries = state.entries.filter((entry) => state.picked.has(entry.id));
  const title = conversationTitle();
  const base = `${safeName(title)}-${stamp()}`;

  state.busy = true;
  let failure = null;
  let ok = false;
  syncCount();
  try {
    if (state.format === "md") {
      const blob = new Blob([toMarkdown(title, entries)], { type: "text/markdown;charset=utf-8" });
      download(blob, `${base}.md`);
    } else {
      const blob = await renderConversationToPng({
        title,
        meta: `${T.exported} · ${new Date().toLocaleString(isZh ? "zh-CN" : undefined)}`,
        messages: entries.map((entry) => ({
          role: entry.role,
          label: entry.label,
          time: entry.time,
          text: entry.text,
        })),
        footer: T.exported,
      });
      download(blob, `${base}.png`);
    }
    ok = true;
  } catch (error) {
    console.error("[mirobody] export failed", error);
    failure = error && error.message === "TOO_TALL" ? T.tooLong : T.failed;
  } finally {
    if (state) state.busy = false;
    if (ok) {
      // The panel stays up for a moment on purpose: the download lands in the
      // browser's downloads folder, and a panel that vanishes silently reads
      // as "nothing happened".
      const button = document.querySelector(".mcs-primary");
      if (button) {
        button.textContent = T.done;
        button.disabled = true;
      }
      const count = document.querySelector(".mcs-count");
      if (count) count.textContent = T.done;
      setTimeout(closePanel, 1400);
    } else if (failure) {
      if (state) syncCount();
      const count = document.querySelector(".mcs-count");
      if (count) {
        count.textContent = failure;
        count.style.color = "#c0392b";
      }
    }
  }
}

// ── mount ──────────────────────────────────────────────────────────────────

const SHARE_ICON = `
  <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor"
    stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
    <path d="M4 12v8a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2v-8"/>
    <path d="M16 6l-4-4-4 4"/>
    <path d="M12 2v13"/>
  </svg>`;

function mount() {
  const sid = sessionId();
  const right = headerRight();
  const existing = document.getElementById("mcs-share-btn");

  if (!sid || !right) {
    if (existing) existing.remove();
    return;
  }
  if (existing && existing.parentElement === right) return;
  if (existing) existing.remove();

  const button = document.createElement("button");
  button.id = "mcs-share-btn";
  button.className = "mcs-btn";
  button.type = "button";
  button.title = T.action;
  button.innerHTML = `${SHARE_ICON}<span>${T.action}</span>`;
  button.addEventListener("click", (event) => {
    event.preventDefault();
    event.stopPropagation();
    openPanel();
  });

  // Ahead of the "ask for" picker: it is the widest thing in that corner, and
  // a control added after it would sit half off short viewports.
  right.insertBefore(button, right.firstChild);
}

injectStyle();
mount();
// The client is a SPA: the header is re-created on every navigation and the
// session id lives in the path, so both are watched from one cheap timer.
setInterval(mount, 500);
window.addEventListener("popstate", mount);
