# MCP UI widgets — the ChatGPT Apps surface

These four files are why "works in ChatGPT Apps" is true. They are **MCP
resources** in the [OpenAI Apps SDK](https://developers.openai.com/apps-sdk)
widget format: when a ChatGPT user connects this server (see `MCP_PUBLIC_URL`),
ChatGPT lists these resources, and renders the HTML **inside the conversation**
as the UI for our tool results.

| Pair | Widget |
| --- | --- |
| `chart_url.{json,html}` | renders a chart image/URL returned by a tool, inline in ChatGPT |
| `upload.{json,html}` | the health-data analysis surface for uploaded files |

How a pair works:

- The `.json` is the resource descriptor: an `ui://widget/...` URI, the
  `text/html+skybridge` mime type ChatGPT looks for, and `_meta` keys the Apps
  SDK reads — invocation status strings (`openai/toolInvocation/*`) and the
  widget's Content-Security-Policy allowlist (`openai/widgetCSP`).
- The `.html` (referenced via `_file`) is the widget itself, executed in
  ChatGPT's sandboxed iframe under that CSP.

They are auto-served by the MCP resource loader from `MCP_RESOURCE_DIRS`
(config.yaml) — nothing imports them, which is exactly why this README exists:
deleting them wouldn't break a test, it would break the ChatGPT integration.

Editing rules of thumb: keep the CSP allowlist as tight as the widget allows;
any new domain the HTML fetches from must be added to `openai/widgetCSP` or
ChatGPT will block the request silently.
