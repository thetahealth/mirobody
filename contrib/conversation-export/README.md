# Export a conversation

A Share button for the chat header: tick the messages you want, pick a format,
get a file.

| Format | What comes out |
| --- | --- |
| Markdown | `# title`, then one `## role · time` section per message — paste-able into notes, issues, a paper appendix |
| Image | an 880 px PNG poster at 2×, white paper, role chips, the same markdown rendered (headings, lists, quotes, tables, code, bold, inline code) |

Both are produced in the browser. Nothing is uploaded, and no new endpoint is
called: the transcript comes from `GET /api/history?session_id=` with the
`ACCESS_TOKEN` the page already holds, exactly as the client asks for it.

## Why it is not inside `frontend/`

Because that directory is build output. [docs/frontend.md](../../docs/frontend.md)
says so — the client's source lives in another repository, and the documented
way to sync a build into a checkout is

```bash
rm -rf ../mirobody/frontend/assets && rsync -a --delete dist/ ../mirobody/frontend/
```

`--delete` removes anything the new build does not contain, so a file added to
`frontend/` is deleted by the next sync. Patching `assets/index-*.js` is worse:
the name changes every build. An add-on outside the directory is the only form
this can take without the client's source.

## Install

1. Chrome / Edge → `chrome://extensions` → turn on **Developer mode** →
   **Load unpacked** → pick this folder.
2. Open a conversation (`/ask/<session id>` or `/chat/<session id>`), signed in.
   The button sits at the left edge of the header's right-hand cluster, before
   the model picker.
3. Any other host — the hosted app, a LAN address — add it to `matches` in
   `manifest.json`, both in `content_scripts` and in `web_accessible_resources`,
   then reload the extension. A match pattern has no port, so
   `https://app.mirobody.ai/*` covers whatever port the deployment uses.

`content.js` checks `/mirobody.json` before doing anything, so the extension is
inert on any other site even if you widen the match pattern.

## What it hooks into

Two things, both chosen to survive a client rebuild:

| | |
| --- | --- |
| `GET /api/history?session_id=` | documented HTTP surface; returns `data.history[]` with `role`, `content`, `timestamp`, `content_dict` |
| `[class*="_header_"]`, `[class*="_right_"]`, `[class*="_title_"]` | class-name *fragments*, not hashed names — the hash suffix is what changes between builds |
| `localStorage.ACCESS_TOKEN` | the token the client itself stores |

That last row is also the security boundary: the add-on can only read the
signed-in user's own conversations, because it sends the same bearer token the
page sends and the server authorises it the same way. There is no new surface to
authorise, audit or rate-limit.

## Limits

- **One image, not a series.** Chromium refuses a canvas side over ~32k px. The
  poster drops from 2× to 1× when it would cross that, and past it the export
  refuses with a message rather than producing a truncated file — select fewer
  messages.
- **Chat routes only.** The button appears on `/ask/<id>` and `/chat/<id>`; a
  fresh conversation has no id yet, so there is nothing to export.
- **Theme-independent output.** The poster is always the light paper style.
  That is deliberate: a shared image should not depend on whether the reader had
  dark mode on.
- **Attachments are named, not embedded.** A message with files exports as
  `[Attachment] report.pdf` — the bytes stay in Mirobody.

## Porting it into the client

If you have the client's source, the interesting file is
[`chat-share-image.js`](chat-share-image.js): `renderConversationToPng({title,
meta, messages, footer})` → `Blob`, no dependencies, ~600 lines, and the
markdown subset it understands is the one listed at the top of that file.
[`chat-share.js`](chat-share.js) is the picker and the two download paths, and
is worth reading mainly for the panel's behaviour (filter chips, the empty-selection
guard, the deliberate 1.4 s pause after a successful export so the download is
not silent).
