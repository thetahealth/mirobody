/**
 * Conversation → PNG, drawn on a canvas.
 *
 * Used by `chat-share.js`; importable on its own if you want the poster
 * without the picker. It is deliberately dependency-free: the shipped bundle
 * carries no html2canvas, and a screenshot of the DOM would drag the app's
 * theme, zoom level and scroll position into the picture. Painting from the
 * transcript text keeps the output stable — the same conversation produces the
 * same image on any machine.
 *
 * Supported markdown is what an agent answer actually contains: headings,
 * paragraphs, bullet/numbered lists, block quotes, fenced code, tables, rules,
 * and inline **bold** / `code` / *italic* / [links](…).
 */

export const POSTER_WIDTH = 880;
const PAD = 56;
const BODY_SIZE = 15.5;
const LINE_HEIGHT = 1.75;

export const FONT_STACK =
  '"Source Sans 3","PingFang SC","Microsoft YaHei","Noto Sans SC",system-ui,sans-serif';
const MONO_STACK = '"Geist Mono",ui-monospace,SFMono-Regular,Consolas,monospace';

const C = {
  paper: "#ffffff",
  frame: "#e9e3d9",
  ink: "#221f1b",
  inkSoft: "#6b6660",
  inkFaint: "#9a948c",
  accent: "#0e3880",
  userBubble: "#f6f3ed",
  userChip: "#e8e1d5",
  codeBg: "#f7f5f1",
  rule: "#e7e2d9",
  tableHead: "#f1ede6",
};

const font = (size, weight = 400) => `${weight} ${size}px ${FONT_STACK}`;
const mono = (size, weight = 400) => `${weight} ${size}px ${MONO_STACK}`;

// ── inline markdown ────────────────────────────────────────────────────────

/** Split one line into styled runs. Unknown syntax stays literal text. */
function inlineRuns(text) {
  const runs = [];
  let rest = text;
  const push = (t, style) => {
    if (!t) return;
    const last = runs[runs.length - 1];
    if (last && last.style === style) last.text += t;
    else runs.push({ text: t, style });
  };

  // `[label](url)` → label; the URL is noise in a picture of a conversation.
  rest = rest.replace(/\[([^\]]+)\]\((?:[^)]*)\)/g, "$1");

  const pattern = /(\*\*[^*]+\*\*|__[^_]+__|`[^`]+`|\*[^*\n]+\*)/g;
  let index = 0;
  let match;
  while ((match = pattern.exec(rest))) {
    push(rest.slice(index, match.index), "plain");
    const token = match[0];
    if (token.startsWith("**") || token.startsWith("__")) {
      push(token.slice(2, -2), "bold");
    } else if (token.startsWith("`")) {
      push(token.slice(1, -1), "code");
    } else {
      push(token.slice(1, -1), "italic");
    }
    index = match.index + token.length;
  }
  push(rest.slice(index), "plain");
  return runs.length ? runs : [{ text, style: "plain" }];
}

function runFont(style, base = BODY_SIZE, baseWeight = 400) {
  if (style === "code") return mono(base - 0.5);
  if (style === "bold") return font(base, 700);
  if (style === "italic") return font(base, 400);
  return font(base, baseWeight);
}

// ── block parsing ──────────────────────────────────────────────────────────

function parseBlocks(markdown) {
  const lines = String(markdown || "").replace(/\r\n?/g, "\n").split("\n");
  const blocks = [];
  let i = 0;

  while (i < lines.length) {
    const line = lines[i];

    if (!line.trim()) {
      i += 1;
      continue;
    }

    // fenced code
    const fence = line.match(/^\s*```(\w*)\s*$/);
    if (fence) {
      const body = [];
      i += 1;
      while (i < lines.length && !/^\s*```\s*$/.test(lines[i])) {
        body.push(lines[i]);
        i += 1;
      }
      i += 1; // closing fence
      blocks.push({ kind: "code", text: body.join("\n") });
      continue;
    }

    // rule
    if (/^\s*([-*_])\1{2,}\s*$/.test(line)) {
      blocks.push({ kind: "rule" });
      i += 1;
      continue;
    }

    // heading
    const heading = line.match(/^\s*(#{1,6})\s+(.*)$/);
    if (heading) {
      blocks.push({ kind: "heading", level: heading[1].length, text: heading[2].trim() });
      i += 1;
      continue;
    }

    // table: a header row followed by a separator row
    if (/^\s*\|.*\|\s*$/.test(line) && /^\s*\|[\s:-]*\|[\s:|-]*$/.test(lines[i + 1] || "")) {
      const rows = [];
      while (i < lines.length && /^\s*\|.*\|\s*$/.test(lines[i])) {
        if (!/^\s*\|[\s:-]*\|[\s:|-]*$/.test(lines[i])) {
          rows.push(
            lines[i]
              .trim()
              .replace(/^\||\|$/g, "")
              .split("|")
              .map((cell) => cell.trim())
          );
        }
        i += 1;
      }
      blocks.push({ kind: "table", rows });
      continue;
    }

    // quote (consecutive `>` lines belong together)
    if (/^\s*>\s?/.test(line)) {
      const body = [];
      while (i < lines.length && /^\s*>\s?/.test(lines[i])) {
        body.push(lines[i].replace(/^\s*>\s?/, ""));
        i += 1;
      }
      blocks.push({ kind: "quote", runs: inlineRuns(body.join(" ")) });
      continue;
    }

    // lists
    const bullet = line.match(/^(\s*)[-*+]\s+(.*)$/);
    const ordered = line.match(/^(\s*)(\d+)[.)]\s+(.*)$/);
    if (bullet || ordered) {
      const items = [];
      let orderedList = Boolean(ordered);
      while (i < lines.length) {
        const b = lines[i].match(/^(\s*)[-*+]\s+(.*)$/);
        const o = lines[i].match(/^(\s*)(\d+)[.)]\s+(.*)$/);
        if (!b && !o) {
          // an indented continuation belongs to the item above it, not to a
          // new paragraph — dropping that made wrapped list text jump left
          if (/^\s{2,}\S/.test(lines[i]) && items.length) {
            items[items.length - 1].runs.push({ text: " ", style: "plain" });
            items[items.length - 1].runs.push(...inlineRuns(lines[i].trim()));
            i += 1;
            continue;
          }
          break;
        }
        if (b) {
          items.push({ depth: Math.min(2, Math.floor(b[1].length / 2)), runs: inlineRuns(b[2]) });
        } else {
          orderedList = true;
          items.push({
            depth: Math.min(2, Math.floor(o[1].length / 2)),
            marker: `${o[2]}.`,
            runs: inlineRuns(o[3]),
          });
        }
        i += 1;
      }
      blocks.push({ kind: "list", ordered: orderedList, items });
      continue;
    }

    // paragraph: until a blank line or the start of another block
    const body = [];
    while (
      i < lines.length &&
      lines[i].trim() &&
      !/^\s*(#{1,6}\s|```|>|[-*+]\s|\d+[.)]\s|\|.*\|)\s*/.test(lines[i]) &&
      !/^\s*([-*_])\1{2,}\s*$/.test(lines[i])
    ) {
      body.push(lines[i]);
      i += 1;
    }
    if (!body.length) {
      // A line that only looks like a block start; keep it rather than loop.
      body.push(lines[i]);
      i += 1;
    }
    blocks.push({ kind: "paragraph", runs: inlineRuns(body.join(" ")) });
  }

  return blocks;
}

// ── text wrapping ──────────────────────────────────────────────────────────

/** Tokens keep CJK characters breakable one by one, words whole. */
function tokenize(runs) {
  const tokens = [];
  for (const run of runs) {
    const pieces = run.text.match(/[\u3000-\u9fff\uff00-\uffef]|[^\s\u3000-\u9fff\uff00-\uffef]+|\s+/g) || [];
    for (const piece of pieces) {
      tokens.push({ text: piece, style: run.style, space: /^\s+$/.test(piece) });
    }
  }
  return tokens;
}

/** Greedy wrap; returns lines of `{text, style}` runs that fit `maxWidth`. */
function wrapRuns(ctx, runs, maxWidth) {
  const lines = [];
  let line = [];
  let width = 0;

  const flush = () => {
    // trailing spaces never render at a line end
    while (line.length && line[line.length - 1].space) line.pop();
    lines.push(line);
    line = [];
    width = 0;
  };

  for (const token of tokenize(runs)) {
    ctx.font = runFont(token.style);
    const w = ctx.measureText(token.text).width;
    // A URL or a hashed file name has no break opportunity; without this it
    // would run off the poster instead of wrapping.
    if (w > maxWidth && !token.space) {
      for (const ch of token.text) {
        const cw = ctx.measureText(ch).width;
        if (width + cw > maxWidth && line.length) flush();
        line.push({ text: ch, style: token.style });
        width += cw;
      }
      continue;
    }
    if (!token.space && width + w > maxWidth && line.length) flush();
    if (token.space && !line.length) continue; // no leading space on a new line
    line.push(token);
    width += w;
  }
  if (line.length) flush();
  return lines.length ? lines : [[]];
}

function drawRunLine(ctx, line, x, y, size = BODY_SIZE) {
  let cursor = x;
  for (const token of line) {
    ctx.font = runFont(token.style, size);
    if (token.style === "code") {
      const w = ctx.measureText(token.text).width;
      ctx.fillStyle = C.codeBg;
      ctx.fillRect(cursor - 2, y - size, w + 4, size + 6);
      ctx.fillStyle = C.accent;
      ctx.fillText(token.text, cursor, y);
    } else {
      ctx.fillStyle = C.ink;
      ctx.fillText(token.text, cursor, y);
    }
    cursor += ctx.measureText(token.text).width;
  }
  return cursor;
}

// ── layout + paint ─────────────────────────────────────────────────────────

/**
 * Everything is measured before a canvas exists: the poster height is only
 * known once the text has been wrapped, and a canvas cannot grow after paint.
 */
function buildOps(ctx, header, messages, footer) {
  const ops = [];
  const width = POSTER_WIDTH;
  const contentWidth = width - PAD * 2;
  let y = PAD;

  // header — brand mark + title + meta
  ops.push({ t: "rect", x: PAD, y: y + 2, w: 26, h: 26, r: 7, fill: C.accent });
  ops.push({ t: "text", x: PAD + 9, y: y + 20, text: "M", font: font(15, 700), fill: "#fff" });
  ops.push({ t: "text", x: PAD + 36, y: y + 15, text: "Mirobody", font: font(13, 600), fill: C.inkSoft });
  ops.push({
    t: "text",
    x: PAD + 36,
    y: y + 32,
    text: header.meta,
    font: font(11.5),
    fill: C.inkFaint,
  });
  y += 58;

  ctx.font = font(26, 700);
  {
    // wrap at heading size: the body-size wrap above would break the title
    // somewhere else entirely
    const rows = [];
    let line = "";
    for (const token of tokenize([{ text: header.title, style: "plain" }])) {
      const candidate = line + token.text;
      if (ctx.measureText(candidate).width > contentWidth && line) {
        rows.push(line);
        line = token.text.trimStart();
      } else {
        line = candidate;
      }
    }
    if (line) rows.push(line);
    for (const row of rows) {
      ops.push({ t: "text", x: PAD, y: y + 22, text: row, font: font(26, 700), fill: C.ink });
      y += 36;
    }
    if (!rows.length) y += 10;
  }

  y += 14;
  ops.push({ t: "rect", x: PAD, y, w: contentWidth, h: 1, fill: C.rule });
  y += 30;

  // messages
  for (const message of messages) {
    const isUser = message.role === "user";
    const bubbleStart = y;
    const bubbleAt = ops.length;

    // role chip
    const chipText = message.label;
    ctx.font = font(11.5, 600);
    const chipW = ctx.measureText(chipText).width + 20;
    ops.push({
      t: "rect",
      x: PAD,
      y: y - 12,
      w: chipW,
      h: 22,
      r: 11,
      fill: isUser ? C.userChip : "rgba(14,56,128,0.09)",
    });
    ops.push({
      t: "text",
      x: PAD + 10,
      y: y + 3,
      text: chipText,
      font: font(11.5, 600),
      fill: isUser ? C.inkSoft : C.accent,
    });
    if (message.time) {
      ops.push({
        t: "text",
        x: PAD + chipW + 10,
        y: y + 3,
        text: message.time,
        font: font(11),
        fill: C.inkFaint,
      });
    }
    y += 30;

    const blocks = parseBlocks(message.text);
    for (const block of blocks) {
      y = layoutBlock(ctx, block, ops, y, PAD, contentWidth);
    }

    // The question sits in a tinted card, the answer in open page: that is
    // the one thing that separates the two voices at a glance on a poster.
    if (isUser) {
      ops.splice(bubbleAt, 0, {
        t: "rect",
        x: PAD - 14,
        y: bubbleStart - 16,
        w: contentWidth + 28,
        h: y - bubbleStart + 16,
        r: 12,
        fill: C.userBubble,
      });
    }
    y += 26;
  }

  // footer
  y += 4;
  ops.push({ t: "rect", x: PAD, y, w: contentWidth, h: 1, fill: C.rule });
  y += 22;
  ops.push({ t: "text", x: PAD, y, text: footer, font: font(11), fill: C.inkFaint });
  y += 18;

  return { ops, height: Math.ceil(y + PAD - 10) };
}

function layoutBlock(ctx, block, ops, y, x, width) {
  switch (block.kind) {
    case "heading": {
      const size = block.level <= 2 ? 19 : 16.5;
      ctx.font = font(size, 700);
      const rows = [];
      let line = "";
      for (const token of tokenize([{ text: block.text, style: "plain" }])) {
        const candidate = line + token.text;
        if (ctx.measureText(candidate).width > width && line) {
          rows.push(line);
          line = token.text.trimStart();
        } else line = candidate;
      }
      if (line) rows.push(line);
      y += 8;
      for (const row of rows) {
        ops.push({ t: "text", x, y: y + size * 0.95, text: row, font: font(size, 700), fill: C.ink });
        y += size * 1.5;
      }
      return y + 8;
    }

    case "paragraph": {
      const lines = wrapRuns(ctx, block.runs, width);
      for (const line of lines) {
        y += BODY_SIZE * LINE_HEIGHT;
        ops.push({ t: "runline", line, x, y });
      }
      return y + 8;
    }

    case "list": {
      for (const item of block.items) {
        const indent = 20 + item.depth * 20;
        const marker = item.marker || "•";
        // right-aligned so "9." and "10." keep the same gap to the text
        ops.push({
          t: "text",
          x: x + indent - 8,
          y: y + BODY_SIZE * LINE_HEIGHT,
          text: marker,
          font: font(BODY_SIZE),
          fill: C.inkSoft,
          align: "right",
        });
        const lines = wrapRuns(ctx, item.runs, width - indent);
        for (const line of lines) {
          y += BODY_SIZE * LINE_HEIGHT;
          ops.push({ t: "runline", line, x: x + indent, y });
        }
      }
      return y + 8;
    }

    case "quote": {
      const lines = wrapRuns(ctx, block.runs, width - 18);
      const startY = y;
      for (const line of lines) {
        y += BODY_SIZE * LINE_HEIGHT;
        ops.push({ t: "runline", line, x: x + 18, y });
      }
      ops.push({ t: "rect", x, y: startY + 4, w: 3, h: y - startY - 2, r: 2, fill: "#d8d2c7" });
      return y + 10;
    }

    case "code": {
      const size = 13;
      ctx.font = mono(size);
      const lines = [];
      for (const raw of block.text.split("\n")) {
        let line = "";
        for (const ch of raw) {
          if (ctx.measureText(line + ch).width > width - 28 && line) {
            lines.push(line);
            line = ch;
          } else line += ch;
        }
        lines.push(line);
      }
      const boxH = lines.length * size * 1.6 + 20;
      ops.push({ t: "rect", x, y: y + 4, w: width, h: boxH, r: 8, fill: C.codeBg, stroke: C.rule });
      let ly = y + 4 + 10 + size;
      for (const line of lines) {
        ops.push({ t: "text", x: x + 14, y: ly, text: line, font: mono(size), fill: "#3d3833" });
        ly += size * 1.6;
      }
      return y + boxH + 14;
    }

    case "table": {
      const cols = Math.max(...block.rows.map((r) => r.length));
      const cellPad = 10;
      const colWidth = width / cols;
      ctx.font = font(13.5);
      let rowIndex = 0;
      for (const row of block.rows) {
        // every cell is wrapped first so the whole row can size to the tallest
        const cellLines = [];
        let rowLines = 1;
        for (let c = 0; c < cols; c += 1) {
          const wrapped = wrapRuns(ctx, inlineRuns(row[c] || ""), colWidth - cellPad * 2);
          cellLines.push(wrapped);
          rowLines = Math.max(rowLines, wrapped.length);
        }
        const rowH = rowLines * 13.5 * 1.55 + 14;
        if (rowIndex === 0) {
          ops.push({ t: "rect", x, y, w: width, h: rowH, fill: C.tableHead });
        }
        for (let c = 0; c < cols; c += 1) {
          // each column keeps its own cursor: sharing one advanced every cell
          // of column c+1 by the lines column c had already drawn
          let cy = y + 8 + 13.5;
          for (const line of cellLines[c]) {
            const runs = rowIndex === 0
              ? line.map((token) => ({ ...token, style: "bold" }))
              : line;
            ops.push({ t: "runline", line: runs, x: x + c * colWidth + cellPad, y: cy, size: 13.5 });
            cy += 13.5 * 1.55;
          }
        }
        y += rowH;
        ops.push({ t: "rect", x, y, w: width, h: 1, fill: C.rule });
        rowIndex += 1;
      }
      return y + 14;
    }

    case "rule":
      y += 12;
      ops.push({ t: "rect", x, y, w: width, h: 1, fill: C.rule });
      return y + 18;

    default:
      return y;
  }
}

function paint(canvas, ops, width, height, scale) {
  const ctx = canvas.getContext("2d");
  canvas.width = width * scale;
  canvas.height = height * scale;
  ctx.scale(scale, scale);

  ctx.fillStyle = C.paper;
  ctx.fillRect(0, 0, width, height);

  ctx.textBaseline = "alphabetic";
  for (const op of ops) {
    if (op.t === "rect") {
      ctx.beginPath();
      const r = Math.min(op.r || 0, op.h / 2, op.w / 2);
      if (r > 0) {
        ctx.moveTo(op.x + r, op.y);
        ctx.arcTo(op.x + op.w, op.y, op.x + op.w, op.y + op.h, r);
        ctx.arcTo(op.x + op.w, op.y + op.h, op.x, op.y + op.h, r);
        ctx.arcTo(op.x, op.y + op.h, op.x, op.y, r);
        ctx.arcTo(op.x, op.y, op.x + op.w, op.y, r);
        ctx.closePath();
      } else {
        ctx.rect(op.x, op.y, op.w, op.h);
      }
      ctx.fillStyle = op.fill || "transparent";
      if (op.fill) ctx.fill();
      if (op.stroke) {
        ctx.strokeStyle = op.stroke;
        ctx.lineWidth = 1;
        ctx.stroke();
      }
    } else if (op.t === "text") {
      ctx.font = op.font;
      ctx.fillStyle = op.fill;
      ctx.textAlign = op.align || "left";
      ctx.fillText(op.text, op.x, op.y);
      ctx.textAlign = "left";
    } else if (op.t === "runline") {
      drawRunLine(ctx, op.line, op.x, op.y, op.size);
    }
  }
}

// ── public ─────────────────────────────────────────────────────────────────

export async function renderConversationToPng({ title, meta, messages, footer, scale = 2 }) {
  if (document.fonts && document.fonts.ready) {
    try {
      await document.fonts.ready;
    } catch {
      /* a font that never settles is not worth blocking the export */
    }
  }

  const probe = document.createElement("canvas").getContext("2d");
  probe.font = font(BODY_SIZE);
  const { ops, height } = buildOps(probe, { title, meta }, messages, footer);

  // Chromium refuses a canvas whose side exceeds ~32k px, and a long
  // conversation reaches that easily. Dropping to 1x buys half the height
  // back; past that the caller has to export fewer messages.
  const MAX_SIDE = 30000;
  let effectiveScale = scale;
  while (height * effectiveScale > MAX_SIDE && effectiveScale > 1) {
    effectiveScale = Math.max(1, effectiveScale - 0.5);
  }
  if (height * effectiveScale > MAX_SIDE) {
    throw new Error("TOO_TALL");
  }

  const canvas = document.createElement("canvas");
  paint(canvas, ops, POSTER_WIDTH, height, effectiveScale);

  return new Promise((resolve, reject) => {
    canvas.toBlob((blob) => (blob ? resolve(blob) : reject(new Error("canvas.toBlob failed"))), "image/png");
  });
}

export const POSTER_COLORS = C;
export { C as COLORS };
