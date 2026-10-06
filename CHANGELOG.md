## Unreleased

### Upgrade notes

- **The reading tables 1.5.0 replaced are dropped.** `th_series_dim`,
  `fhir_indicators` and `standard_indicators_device` never held a reading and
  go at the first boot. `th_series_data` (renamed `th_series_data_retired_15`
  in 1.5.0), which held every reading before 1.5.0, goes only once every row
  in it is proven moved: `mirobody migrate-observations` marks a row moved
  when a row with the same fingerprint is in the observation model, and the
  table is dropped, by that command or at the next boot, when no other row is
  left. Rows the person deleted go with it. Upgrading from 1.4.x, or from
  1.5.0–1.5.3 if the table is still there: after the first boot, run
  `docker compose exec mirobody mirobody migrate-observations` until it
  reports the table dropped; it says which rows keep it and what to do. If
  you ran it on 1.5.0–1.5.3 and have erased readings since, run it with
  `--verify-only` first: nothing recorded those erasures, so a plain run would
  write them back. That run writes nothing, marks what is already moved and
  counts the rest.

### Added

- **`skills/`: two skills for someone else's agent.** `npx skills add
  thetahealth/mirobody --skill <name>` drops one into Claude Code, Codex,
  Cursor or Gemini CLI. `translate-health-data` turns lab documents, an Apple
  Health export, symptoms and units into LOINC, UCUM and ICPC-3 rows and FHIR
  Observations; `mirobody` runs the Docker stack and connects an agent to it
  over MCP. `.claude-plugin/marketplace.json` offers the same two as a plugin
  for Claude Code and for Codex, which reads that file as it is.
  `mirobody/tests/test_skills.py`
  re-runs every code, subcommand, Compose service and number the prose
  quotes, and pins six category terms that must stay deliberately unresolved.
  The module ships in the wheel and sdist as inspectable release evidence.
- **The first start asks for a model in the browser.** With no key in
  `.env` the stack still starts, and `./deploy.sh` prints a link to `/setup`:
  paste one vendor key, kept only after a real request through it works, or
  choose 100% on this machine. The choice is stored encrypted, applies without
  a restart and never overrides `.env` (nor a key `.env` holds under another
  name, `GEMINI_API_KEY` for `GOOGLE_API_KEY`). Saving takes the
  `SETUP_TOKEN` `deploy.sh` writes, and once a model is set up, a signed-in
  session too: the server prints the link only while none is. A key is
  checked with one real request before it is kept, without changing what
  other requests read meanwhile. Settings › Model returns to the page.
- **A model name is yours to change.** Model names change faster than
  releases. The setup page shows the model beside each key and on the local
  server, and takes another (a local server's are listed to pick from); a
  vendor model is checked with one real request first. In `.env` the same is
  one line, the variable each `config.llm.yaml` entry names as its
  `model_env` (`OPENROUTER_CHAT_MODEL`, `LOCAL_MODEL`, `LOCAL_OCR_MODEL`, …),
  and `deploy.sh` copies it from the command line like a key.
- **Every model can run on the same machine.** `LOCAL_BASE_URL` and
  `LOCAL_OCR_BASE_URL` point the `local` entries at any OpenAI-compatible
  server. The shipped preset runs on llama.cpp: GLM-OCR-0.9B reads
  documents, and one of two models answers, picked on the setup page by what
  each downloads and needs. MiniCPM5-2B, the default: 3.0 GB with the
  reader, 5.7 GB of memory at most, 28 s a median answer on a 16 GB M1 Pro,
  16 of 24 evaluation questions passed and 14 answered with every expected
  fact. Qwen3.8-27B: 14.5 GB, about 20 GB of memory; on an Apple M4 Pro 16 of
  16 earlier test questions with no number the record lacks, about two
  minutes an answer, 27 of 27 demo readings. `docker compose --profile
  local` (NVIDIA) or `--profile local-cpu` runs it next to the app. Each
  model server keeps at most 1 GiB of prompt cache, the reader none:
  llama.cpp's default is 8 GiB per model, and two small models filled a
  16 GB Mac's disk with swap. Both slots of a model share one KV pool
  (`kv-unified`), so one long question can use the whole context: split, a
  32k model answered in a 16k slot and two evaluation questions ended in
  `ContextOverflowError`, at the same memory. The evaluation, its seed and how to rerun it
  are in `benchmarks/local_models/`; `docs/local-models.md` is the guide.
  The models download from Hugging Face the first time; `HF_ENDPOINT` in
  `.env` points the `llama` service at a mirror.
- **A table is read by its header, without a model**, when a document-OCR
  model is routed (the local setup). Rows under a header the rules know
  (项目名称 / 结果 / 参考值 / 单位, Analyte / Result / Unit, a CSV's first
  line), including two panels side by side, are stored as printed and
  labelled `rules:table@v1`, with only the flag the report printed. A row is
  read only when it looks like a reading (a number, `1+` or 阴性 beside a
  unit and a range); patient details are skipped; a value the page's other
  copy (the text layer, or the OCR's text pass) does not confirm, a table of
  one row per day, and any text outside the tables that holds a number or a
  finding go to the text model.
- **`mirobody doctor --probe` sends one real request per surface** (a tool
  call, a schema-bound answer, an image, the OCR passes) and checks that each
  local server runs the model its entry names. `doctor` reads the setup
  page's choice as the server does, so a deployment set up in the browser is
  not reported as having no model.
- **`POST /files/upload?file=true` files what it stores.** Without the flag
  the route only stores the file, the first step of a chat attachment that
  the turn then files, and its message now says so instead of "uploaded
  successfully". With it, the files land in the record and extraction
  starts, as a Data-page upload does.

### Security

Each item below was reproduced on a running server on 2026-10-01 before its
fix, and re-run after it; `mirobody/tests/test_security_gates.py` pins the
decisions.

- **MFA covers file links and the upload socket.** The JWT middleware asks
  for a second factor only of a token in the Authorization header. A file
  link (`GET /files/{path}?access_token=`) and the upload socket (`?token=`)
  take their token from the query string, so with MFA on, an account's
  code-only token (`aal` 1) still read its lab report and opened the socket.
  Both now ask the same rule (`middlewares.lacks_second_factor`): the link
  answers 403 `ERROR_AAL2_REQUIRED`, and the socket is closed with 1008.
- **Passkeys and MFA work once `WEBAUTHN_RP_ID` is set.** `Server.start()`
  never passed `config.get_webauthn_options()`, so the setting never reached
  the server. Settings offered passkeys (`webauthn_supported: true`), but
  there was no WebAuthn service to enrol with, and an account with
  `mfa_enabled` was never asked for a second factor. A deployment that sets
  `WEBAUTHN_RP_ID` now has WebAuthn, and an account that already has MFA on
  and a passkey registered is asked for it. To tell: `/mirobody.json` says
  `__IS_WEBAUTHN_ON__: true`.
- **An upload belongs to the account that started it.** Upload sessions are
  keyed by the client's `messageId`, and the chunk, end and status handlers
  never checked whose session it was. A second account's socket read another
  account's upload status, and pushed a chunk that was processed and stored
  as that account's file. Each handler now checks the session's account:
  another account gets "Invalid upload session" or `not_found`, and cannot
  reuse an id another account holds.
- **The vendor callback redirects only within the deployment.**
  `/api/v1/pulse/{platform}/{provider}/callback?state=success&return_url=`
  answered 302 to any URL, unauthenticated. It now redirects to a path on
  this origin, this origin, the CORS origin, or what the new
  `OAUTH_RETURN_ORIGINS` lists (`utils/http.safe_return_url`). Anything else
  gets the completion page. Not changed: the OAuth `state` is still bound to
  the account, not to the browser that started the link.
- **A placeholder `JWT_KEY` never signs a token.** On loopback the server
  kept the shipped placeholder and only warned, and a token minted with it
  read a demo account's files. A reverse proxy on the same machine puts a
  loopback server on the internet. Every run without a real key now gets
  one made for it, wherever it listens, so sessions end at restart until
  `JWT_KEY` is set. `deploy.sh` sets one.
- **Response headers, and no API docs in production.** No response carried
  `nosniff`, a frame policy or a referrer policy, and the referrer matters
  here: `/mcp/<token>` and `/share/<id>` are credentials. Every response now
  carries `X-Content-Type-Options: nosniff`, `X-Frame-Options: SAMEORIGIN`
  and `Referrer-Policy: same-origin`. With `PRODUCTION: true`, `/docs`,
  `/redoc` and `/openapi.json` answer 404.
- **Logs and error replies carry no storage keys or exception text.** A
  storage key names its owner (`demo/<email>/...`). The file route and local
  storage logged keys whole. Three user routes logged `str(e)` with a
  traceback, and four returned it to the caller; a driver's message quotes
  the SQL with its bound parameters. They now log a key fingerprint and the
  exception's type, and answer with a fixed message. The PHI baseline loses
  nine entries.

### Changed

- **The cloud models are the ones vendors ship now.** `config.llm.yaml` still
  named September's: Claude Sonnet 5 and GPT-5.6 Terra. `claude-sonnet`
  (OpenRouter) now runs `anthropic/claude-sonnet-5.5` and `claude` (Anthropic)
  `claude-sonnet-5-5`; `openai` runs `gpt-6-sol` and `openai-utils`
  `gpt-6-luna`, both at `reasoning_effort: none`, the only effort at which
  GPT-6 takes a function tool on Chat Completions or a `temperature`.
  GPT-6.1 Sol is not the `openai` entry because OpenAI serves its tool calls
  only on the Responses API, and the agent speaks Chat Completions; it is the
  `gpt` entry, on OpenRouter, which carries them.
  `openrouter-utils` asks Gemini 3.8 Flash for `low` reasoning, not `minimal`,
  which Google documents as an error on that model; `claude-sonnet` and `gpt`
  drop a `temperature` that Sonnet 5.5 and GPT-6.1 Sol do not take. Still each
  vendor's newest, so unchanged: `qwen3.8-flash`, `gemini-3.8-flash`,
  `deepseek-flash` (DeepSeek V4.1 Flash), `claude-haiku-4-5-20251001`. A model
  set through an entry's `model_env` variable is kept. To tell: `mirobody
  doctor` names the new ids. Measured 2026-10-06 through the product's own
  calls (a tool call and a second round with its result, a schema-bound
  answer, an image, and the demo photo read and its nine readings extracted):
  every OpenRouter and DashScope entry passes. The OpenAI, Anthropic, Google
  and DeepSeek direct entries were checked against each vendor's
  documentation, not called.
- **The README starts with the first-run page, and says what leaves the
  machine in each mode.** `./deploy.sh` alone is the first command; a GIF
  (English and Chinese) shows the page, a table compares a model key, 100% on
  this machine and the library alone with what each needs, and "Privacy"
  became a per-mode table that names the model download, the image registry
  and its mirror, and what encryption at rest does not cover yet. The README,
  the quickstart, `docs/local-models.md`, the Docker Hub copy and the
  `mirobody` skill name llama.cpp as the local model runtime: the README's
  first paragraph and its mode table say that llama.cpp's `llama-server`
  serves the local models and Mirobody runs none itself. The table gives the
  default size (MiniCPM5-2B with GLM-OCR: 16 GB of memory, no GPU, 3.0 GB to
  download) before the large one; it had given only Qwen3.8-27B's 20 GB.
- **The README links the benchmarks.** ESL-Bench, MedHall-Bench and
  MedHarm-Bench each drew 4,000+ Hugging Face downloads in the 30 days to
  2026-10-01, and none of their cards linked here, nor did either README link
  them, the ESL-Bench paper or `thetahealth/mirobody-eval`. Both editions'
  "Numbers you can check" tables now carry one row for them.
- **Two commands to a running stack.** `deploy.sh` writes a model key given
  in its environment into `.env` (`OPENROUTER_API_KEY=sk-or-... ./deploy.sh`),
  under the variable names `config.llm.yaml` reads, so a first run needs no
  second step; a key added later still goes in `.env`, then
  `docker compose up -d`. The README (both editions), the skills and the
  Docker Hub copy say so, and name the source tarball as the way in without
  Git. To tell: after that one command, `mirobody doctor` names a provider.
- **The sign-in page offers the demo account while it is seeded.** Only
  `deploy.sh`'s last line and the README said `you@mirobody.ai` / `111111`.
  `/mirobody.json` carries `__DEMO_SIGN_IN__` under the seed's own three
  conditions (the flag, not PRODUCTION, a predefined code), and **Use it**
  fills the Email code tab.
- **Settings' "API Config" is off unless the overlay turns it on.**
  `__IS_API_CONFIG_ON__` defaulted to true, so every self-hosted page offered
  a "Server URL" field that points the page at another server; this one
  serves the page from its own origin. A deployment that hosts the bundle
  elsewhere sets it in `MIROBODY_WEB_CONFIG`.
- **MCP setup is written per client.** The README, the Settings hint and the
  `mirobody` skill told people to paste the personal link into Claude
  Desktop, whose custom connectors connect from Anthropic's cloud and cannot
  reach `localhost`. They now give one line each for Claude Code, Codex,
  Cursor and Gemini CLI (the link as it is), Claude Desktop (through
  `npx -y mcp-remote <link>`), and say ChatGPT and claude.ai need an HTTPS
  address. Run against a stack: Codex 0.153 and 0.159, Claude Code, and the
  `mcp-remote` bridge answered with the right readings and files; Gemini
  CLI completed the handshake; Cursor's entry is its documented form.
- **`uvx --python 3.12`.** With a default interpreter older than 3.12, uv
  resolved `mirobody` 1.0.62 (177 dependencies and no `mirobody` command) or
  failed to resolve at all, because PyPI releases before 1.2.1 still declare
  `requires-python >=3.8` / `>=3.11`. The README, the skills, the Docker Hub
  copy and `server.json` (`runtimeArguments`) now pin the interpreter; pip
  installs say Python 3.12+.
- **Skills.** `translate-health-data`: install into a virtual environment on
  Python 3.12+; with no key, never search outside the working directory (two
  agent runs listed `~/.config` and ran `find ~ -name .env`) and resolve the
  rows it read with `resolve_reading`; what `needs-input` with
  `icpc3:no-match` asks for; `standardize_report` over stdio needs `[parse]`
  and a key. `mirobody`: one stack per Compose name, ports only from `.env`,
  the per-client MCP table, three troubleshooting rows, and Claude Code and
  Codex named in its description. The README (both editions) and
  `skills/README.md` give the plugin install for Claude Code
  (`claude plugin marketplace add thetahealth/mirobody`, then
  `claude plugin install mirobody@mirobody`) and for Codex
  (`codex plugin marketplace add …`, then `codex plugin add mirobody@mirobody`).
  To tell: both install from GitHub (Claude Code 2.1.285, Codex 0.159.2), and
  Codex lists `mirobody:translate-health-data` and `mirobody:mirobody`. Asked
  to code a checkup PDF with no key, it read that skill, searched only its
  working directory and coded 9 of 9 rows as the Claude Code run did.
- **The journal has its GIF.** Step 4 of the README types one sentence into
  Data › Records and shows it become a complaint, two readings and a
  medication, with "no fever" kept out (`docs/images/journal-demo.gif` and its
  zh-CN twin).
- **Docs say what the tree does.** AGENTS.md and CONTRIBUTING.md count 232
  tests in three shipped modules (they said 147 in two); `pyproject.toml`'s
  note on extras names the three runtime extras; `demo/README.md` stops
  quoting 1.4.4 and 1.5.0; the README no longer says the docs site is
  rendered from `docs/`, which its MCP page is not.
- **The README starts with the Docker path.** Both editions open with the
  three commands that bring the stack up (`git clone`, `./deploy.sh`, one key
  in `.env` then `docker compose up -d`) and what a running deployment does;
  the library, the ICPC-3 axis and the genetics tools follow, each with a
  runnable example, and the figures link the public suites under
  `benchmarks/`. The docs site links point at the pages that exist rather
  than at redirects, and `docs/`, CONTRIBUTING and the workflow README name
  the benchmarks. `benchmarks/README.md` is new.
- **A release publishes its Docker image.** The GitHub Release is created by
  the workflow's own token, and GitHub starts no workflow from such an event,
  so `docker-hub.yml`'s `release: published` trigger never fired; the 1.5.3
  image was published by hand. `pypi-release.yml` now calls it as a reusable
  workflow once the version is confirmed on PyPI, a final release is also
  tagged `latest`, and the documentation site is told when
  `DOCS_DISPATCH_TOKEN` is set.
- **The Docker Hub page now has one source of truth.** The image workflow
  publishes the short and full repository description from
  `docs/docker-hub-description.md`; a manual dispatch can rebuild a repaired
  source ref under an existing version tag without moving the release tag.
- **Readings carry the printed range and a flag.** Readings and latest
  values from `query_health_indicators` now include `ref`, the range as
  printed (empty when none was), and `flag`: the report's own when a table
  rule read the row, the extracting model's high/low/normal against that
  range otherwise. Without the range a model judged a value against one it
  remembered.
- **`th_series` is gone.** The per-person catalogue was rewritten on every
  write and read by nothing: `catalog()` groups `v_observation` directly,
  150 ms over one person's 149,000 readings. Writes no longer pay for it, and
  the table is dropped at boot.

- **Settings no longer report switches that do not exist.** `GET
  /api/user/settings` answered `privacy` (`dataSharing`, `aiAnalysis`,
  `analyticsTracking`) and `notifications` (`email`, `push`, `weeklyReport`, …)
  as `true` for every account, and `PUT` accepted and dropped them: nothing
  shares, tracks or notifies. Both groups are gone from the answer; a client
  that still sends them is not refused.

### Fixed

- **Any document-OCR model's answer is read: PaddleOCR-VL's and MinerU's
  tables too.** Each OCR pass reached the readers as the model wrote it.
  PaddleOCR-VL-1.6 and MinerU2.5 answer `Table Recognition:` in OTSL
  (`<fcel>…<nl>`), which nothing parsed: on the OCR benchmark's 26 pages
  (benchmarks/local_ocr, 303 printed rows) the table rules read none of
  PaddleOCR's rows, where its text held 300. PaddleOCR also writes units and
  flags as LaTeX (`\(\mu mol/L\)`, `6.49\(\uparrow\)`), which the unit engine
  and the value parser do not read, and its text pass repeated `未见异常` on an
  ECG page until the benchmark's 8,192-token cap; the product sent no cap, so
  a loop ran to the end of the model's context. Every answer is now cleaned
  once in `documents/ocr.py` (`clean_answer`): OTSL becomes an HTML table with
  its spans, LaTeX the characters it typesets (`μmol/L`, `×10^9/L`, `↑`), and
  a line or phrase repeated 64 times in a row one copy, with a warning that
  carries only counts. An OCR model's pass is capped at 8,192 tokens, five
  times the longest answer measured (`unified_file_extract(max_tokens=)`, sent
  under the name the endpoint takes, on the Anthropic backend too). GLM-OCR's
  answers hold none of this and are unchanged. To tell: with PaddleOCR-VL as
  `local-ocr`, a scanned report's stored text holds `<table>` and `μmol/L`,
  not `<fcel>` or `\mu`. `docker/local-models.ini` gains a `paddleocr-vl`
  section; the default reader is still `glm-ocr`.
- **The table rules read an OCR model's grid, not only the printed one.** An
  OCR model does not return the table a report prints. On the same benchmark
  PaddleOCR-VL returned a whole page as one grid: a page's first rows (its
  panel's header on the page before) above the next panel's header, a header
  split over two rows (`血常规 | 英文名称 | 化验结果 | 参考值`, then `检查项目`),
  four header words in one cell, and rows whose empty cells moved (`ALT | 23
  | | U/L | 7~40 | 02 |` under `… | Methodology | Status | Unit | Normal
  Range | Lab`, read as a unit under `Status` and the lab code `02` as the
  range). GLM-OCR and MinerU moved empty cells the same way, and all three
  read a US lab's `In Range | Out of Range` columns as no header. Now cell
  spans keep their columns; a row whose cells contradict their columns is
  laid again by content, in order, and read only when one layout fits best;
  a split header is joined; rows above a table's first header borrow it; a
  page with no header at all is typed by its cells when one column holds
  results, one ranges, and the results sit on their ranges' scale (two
  result columns, this result and the last, still go to the model); a result
  cell that holds its range and unit (`2.873 (0.270 - 4.200)&mIU/L`) is split;
  `Out of Range` is the result column a row's empty `In Range` points to; a
  `±` between a range and a unit is a misread `&`; and `采样日期`, `检查日期`
  and their kin are paperwork. A range under the unit header beside an empty
  range cell, which the previous entry left to the model, is now read in its
  place. Rules-only, all three models, with no row stored that is not a
  printed reading: GLM-OCR 162 → 204 rows (unit and range right 156 → 198),
  PaddleOCR-VL 0 → 276 (149 with the benchmark's own OTSL conversion; unit
  and range right 106 → 262), MinerU 0 → 196 (139); table rows left unread
  69 / 185 / 84 → 26 / 52 / 26; pages read with no model 1 / 0 / 2 → 10 /
  10 / 10. To tell: a check-up page printed `Test Item | Measurement |
  Methodology | Status | Unit | Normal Range | Lab` stores ALT with U/L and
  7–40, labelled `rules:table@v1`.
- **The text model is handed what the rules did not read, not a copy of
  it.** A row the rules read left the model's text only when every number in
  it was a read value under its own name, and an OCR's text pass prints each
  row again with what that test misses: a lab code on every row (`02`), the
  row number, the abbreviation in the name's place (`WBC 6.27 3.50-9.50`), a
  column on its own (`5.73↑`), a previous result. On the benchmark those
  copies were most of the text the 2B model got on pages the rules had read
  whole, so it read them again, and its readings of them are where the end-
  to-end errors came from. A line that only repeats a row the rules read, a
  code or row-number column, a previous-result column, a patient-details row
  and a table left with only its header now stay out of the model's text; a
  line holding anything else (`血糖 4.57 mmol/L` beside a read `尿素 4.57`)
  stays in. Text left for the model: GLM-OCR 15,747 → 5,542 characters,
  PaddleOCR-VL 22,053 → 5,374, MinerU 17,686 → 9,105; every printed row the
  rules leave that was in the model's text before is in it still. To tell: a
  page whose rows the rules read logs `indicators read off tables, no model`.
- **A document with no date is not filed under the prompt's example date.**
  The extraction prompt's worked example said `"date_time": "2024-10-30
  00:00:00"`, and MiniCPM5-2B copied it onto documents that print no date of
  their own: on the evaluation's record (benchmarks/local_models, qa3) a few
  readings were filed under 30 October 2024. The example now describes the
  field instead of giving a date, so a copied value does not parse and the
  file goes under the upload time with the Data page's "which date?". The
  journal's worked examples state two times too; a `when` equal to one of
  them is ignored the same way. To check: no `YYYY-MM-DD` appears in the
  extraction prompt; an undated report asks which date it is.
- **A range printed `120--200` is 120 to 200, and `6.49↑` keeps its
  number.** Two readings came out wrong whatever model read the page, found
  by the OCR benchmark (benchmarks/local_ocr):
  - the range reader took the second dash of a doubled dash as a minus sign,
    so `35.0--45.0`, the way check-up books print a range, was stored as
    -45.0 to 35.0; a doubled dash is now one separator (`-3--3` stays -3 to
    3);
  - a value the model returned with the report's flag still on it (`6.49↑`,
    `5.6 H`) was stored as text with no number, so it never charted and
    never compared with its range: every one of GLM-OCR's 16 value errors
    end to end. The model path now moves a printed flag out of the value
    the way the table rules do, `L` read as litres unless the range says
    low, and keeps the printed flag over the model's `status`.
  To check: `mirobody.translate.parse_range("35.0--45.0")` is `(35.0, 45.0)`;
  a report whose result column prints `6.49↑` stores 6.49 with the flag ↑.
- **Long reports, clinic notes and home logs give their readings.** Three
  shapes of document came back with none:
  - a multi-page report sent to a small model in one request: MiniCPM5-2B
    read the 7-page check-up book of the evaluation (78 printed rows) as
    its header and `"indicators": []`. Text over 3,000 characters with page
    headers is now read a page at a time, two pages at once, and the pages'
    rows are joined: 78 of 78 on the same book;
  - an outpatient note with its vitals in the text (T 36.6℃, BP 111/65, 体重
    84.9kg): the prompt allowed only examination reports and device data.
    Clinical notes and self-measurement logs are now health content, and the
    values they state are readings: 10 rows from that note;
  - a log of twelve morning weights: the extraction had one date per
    document, so twelve weights could only be twelve readings of one day,
    and two equal weights were deduplicated into one. A row now carries the
    date it prints (`date_time`), the reading is filed under it, the same
    value on two mornings is two readings, and a log with no date of its own
    takes its latest row's: 12 rows on 12 mornings.
  Measured with the product's own request on MiniCPM5-2B
  (benchmarks/local_models, seed 7). To check: upload a photo of a weight
  log; the Data page lists one reading per row, each on its own day.
- **A keyword finds a reading whatever its unit and however it is spelled.**
  On the 1.5.4 local-model evaluation, `query_health_indicators(keywords=
  ["triglycerides"])` left out a triglyceride printed as `甘油三酯（TG）` in
  mmol/L, for every model size. Two tiers missed it. The lexical one compared
  the word with its plural `s` against `Triglyceride [Moles/volume] …`, and
  "triglyceride" found it. The code one compared codes: the word resolves to
  2571-8 (mass) and the reading was coded 14927-8 (moles). Words are now
  plural-folded on both sides (`lexical.fold_plural`: `-s`, `-ies`, `-sses`,
  by suffix), and the code tier matches the series the code names
  (`translate.series_of`), which holds mass and moles, with or without a
  method, together. "creatinines", "ferritins", "weights" and "LDLs" missed
  the same way and now find their readings. To tell: with a triglyceride
  stored in mmol/L, `keywords=["triglycerides"]` and `["TG"]` both return it.
- **`FER` is ferritin, and `haemoglobin` is hemoglobin.** A lab slip's `FER
  42.3 ng/mL` was coded 2498-4, Iron: LOINC's French translations name iron
  `Fer`, and the index folds case. A ferritin question then saw July's
  reading and not March's; DeepSeek V4.1 Flash found March only by searching
  the report text. The British `haemoglobin` answered 4548-4, Hemoglobin A1c,
  as `血红蛋白` once did. Both now answer what their spelled-out names answer
  (20567-4 and 718-7), through two rows in `resolver_overrides.tsv`; a French
  report's bare `Fer` now answers ferritin too. Readings already stored keep
  their code until `mirobody recode` runs. To tell: `mirobody resolve FER`
  prints a Ferritin code; resolver coverage is 300/300.
- **The table rules read the headers most reports print.** Their vocabulary
  lacked `检测结果`, `化验结果`, `报告结果`, `本次结果`, `数值`, `测量值`, `检查名称`,
  `测定项目`, `Test Item`, `Tests`, `Items`, `Measured`, `REF.RANGE`,
  `参考值(范围)`, `正常参考值` and every Traditional header, and took `Measurement`
  for the name column. On the local-model evaluation's corpus (seed 7) the
  rules read 21 of its 70 documents; the other lab tables went to the text
  model, which on a 1-2B model reads them less exactly. Those words are now in
  the vocabulary; headers, patient labels and date labels are compared with
  Traditional folded to Simplified; `Measurement` is the result column beside
  a name word and the name column without one; and a name or value column is
  kept only when the cells under it agree, so a header word over the wrong
  cells reads nothing. Flag columns headed `标记`, `提示信息`, `判断`, `Status`
  or `Abnormal` keep their printed flag, a flag after a glued unit
  (`50.5% H`) comes off the value, a unit after `&` is the unit even when it
  starts with a digit (`125-350&10^9/L`), and four row kinds the wider reach
  met are not stored: the report's `异常项目数`, the patient's `Name` inside
  the table, `未做`, and a range under the unit header. Same corpus: 57 of 70
  documents read (Traditional 4 of 4, English 9 of 10), 2,186 rows with units
  1,696 of 1,697 and ranges 2,118 of 2,118, no row stored that is not a
  printed reading, and the readings read before unchanged but two flags the
  `标记` column now keeps. To tell: upload a slip headed `检查名称 | 化验结果 |
  参考值(范围)`; its readings are labelled `rules:table@v1`.
- **The table rules keep a table's unit and range, and store no signer line.**
  On the local-model evaluation (benchmarks/local_models, seed 7), the rules
  read `单位(Unit)`, `正常范围值`, a slip's bare `参考`, a blank header over the
  ranges and ranges printed under `结果提示` as no column, so a book's readings
  were stored with no unit and no range: hemoglobin 140 was `needs-input,
  unit:missing` and uncoded, and a query by its code missed it. A photo's OCR
  put the signer line `检查者：…` inside the result table and it was stored as
  a reading: the patient and paperwork labels matched only a cell that held
  the label alone, and the name after it passed as a result word. Those
  headers are now in the vocabulary; a column under a blank header, or under a
  flag word, is typed by its cells (ranges, or units); a unit printed after
  the range (`<7.00&ng/mL`) is the unit; a label is recognized with its value
  in the same cell; and a word with a colon, a unit alone in the value cell
  (`MCV | fl`) and a panel's `是否异常 | 是` go to the model instead. On the
  21 corpus documents the rules read, rebuilt from the generator's own HTML:
  units 54 → 883 of 883 printed, ranges 54 → 1,005 of 1,005, rows stored that
  are not printed readings 18 → 0, readings coded 573 → 807; the same 21
  documents are read, with the same dates. To tell: upload a report whose
  header is `检测项目 | 测定值 | | 单位(Unit)`; hemoglobin is stored with g/L
  and its range, and coded 718-7.
- **OpenAI models can read uploads and the journal.** With GPT-6 Luna,
  GPT-6 Sol, GPT-6.1 Sol or GPT-5.6 Terra as the utility model (through
  OpenRouter, 2026-10-06), indicator extraction and the journal's sentence
  reader got HTTP 400 on every call: OpenAI's json_schema refuses an object
  that is not closed with `additionalProperties: false` or that leaves a
  property out of `required`, so no upload was read and no sentence was
  journaled. Gemini and DeepSeek accept such schemas, which is how it went
  unnoticed. Every json_schema Mirobody sends is now closed and lists every
  property; a field a document does not show comes back as an empty string,
  which every reader already treats as absent. GPT-6 Luna, GPT-6 Sol and
  GPT-5.6 Terra then read all five rows of the demo lipid CSV; Gemini 3.8
  Flash and DeepSeek V4.1 Flash still pass. The unused
  `PROMPT_EXTRACT_INDICATORS` constant is gone. To check: set
  `OPENROUTER_UTILS_MODEL=openai/gpt-6-luna` and upload
  `demo/upload/you_lipid_panel_2026-08.csv`; five readings are filed.
- **Asking for a view before naming an indicator gets the catalogue, not a
  refusal.** `query_health_indicators(view="latest")` with no `keywords` or
  `indicators` was refused ("the catalogue has one shape"). Small local
  models open that way: MiniCPM5-2B 3 times and MiniCPM5-1B 7 times in the
  2026-10-06 evaluation, then they repeated the refused call until the harness
  stopped it, 6 times, each a model turn. Now any view with nothing selected
  answers with the catalogue and a note that the view was not applied and
  which call to make next, over MCP and in chat alike. To check:
  `query_health_indicators(view="latest")` lists what the person has, with
  `view=latest was not applied` in its notes.
- **A day view over a whole record no longer floods the model's context.**
  `view="day"` with no dates returned every day on file: MiniCPM5-2B got
  13,930 and then 33,657 characters, and paged the second from a file until
  its context overflowed. Minute to month views now keep the newest 92 points
  per indicator (`query.BUCKET_CAP`, the longest three months), cut in SQL,
  marked `truncated`, with a note naming the dates that came back and the
  coarser view that covers more. A year of the demo's daily steps renders in
  3,622 characters instead of 12,346. Day, week and month answers also say
  that each day counts once (its elected value, else its last reading), so a
  month's `avg` that differs from `view="stats"` over the same month has a
  stated reason. To check: `query_health_indicators(keywords=["steps"],
  view="day")` on the demo account answers 92 rows marked `truncated`,
  ending on the last day on file.
- **A chart with a stray brace is drawn, or says it could not be.** The
  answer's charts are JSON the model writes by hand in a ```` ```vis-chart ````
  block. One `}` too many (MiniCPM5-2B on a steps chart, 2026-10-06) and the
  web client drew nothing, with no sign a chart was missing; nothing on the
  server checks the block, and it reaches the browser as it streams, so the
  browser is where it is read. Once the block's closing fence has arrived, the
  client now mends structural slips (extra or missing closing brackets, a
  trailing comma, the JSON fenced a second time inside the block) without
  touching a value, and a block it still cannot draw shows "This chart could
  not be drawn" with its data one click away. A doubled fence no longer turns
  the rest of the answer into a code block. Ships with the next web build in
  `frontend/`. To check: an answer containing a vis-chart block whose JSON
  ends in `}}` draws the chart.
- **The journal reads a sentence on a small local model.** Under the JSON
  schema the journal asks for, MiniCPM5-2B answered `{"entries": []}` for
  "Been leg cramps for 5 days.", and the evaluation's 15 diary sentences
  became 1 entry of 31 (benchmarks/local_models, seed 7); every sentence was
  kept only as a note. Unconstrained, the model found the entries, but it
  started its answer by echoing the schema. The request now shows two worked
  answers first (a symptom with a time, a temperature, someone else's cough,
  a negation, a meal), and MiniCPM5-2B answers each test sentence in 1-4 s
  in the writer's language. DeepSeek V4.1 Flash went from 6 to 8 of 8 test
  sentences right on the same change and Gemini 3.8 Flash stayed at 8.
  Rows carry `llm:journal-sentence@v3`. A measurement whose value does not
  start with its number (`"收缩压 123"`, the name inside the value) is
  reported as having no value instead of stored as an uncoded reading. To
  check: on the local small size, type "Been leg cramps for 5 days." in the
  journal; it is a symptom, not a note.
- **A report date printed as 2026年05月01日 is the report's date.** The
  date reader knew `2026-05-01` and `2026/05/01` only, so a date the model
  copied as printed (年月日, dots: `2024.05.10`, or eight digits as an app
  screenshot prints it: `20260418`) counted as no date and the
  readings went under the upload day, with the Data page asking which date.
  Small local models copy the printed form; MiniCPM5-2B did on an XLSX lab
  slip. The warning for a date it still cannot read logs the date's length,
  not the date. To check: upload a report whose date reads 2024年05月10日;
  its readings are filed on May 10.
- **Quoted genotype rows without their header no longer reach a model.**
  MyHeritage and FamilyTreeDNA quote every field (`"rs4477212","1","82154","AA"`).
  The check for header-stripped genotype rows took quotes off only the ends of a
  line, so such a file read as a spreadsheet: a chat upload was classed `csv`, the
  upload path did not claim it, and its rows went to document extraction and its
  model. Quotes inside a row are now dropped too, and both vendors' shapes are
  pinned in `benchmarks/genomics`. Ordinary quoted lab tables still read as
  spreadsheets. To tell: such a file is refused as a genotype export.
- **Device setup on Docker works as the guide says.** `docs/provider-setup.md` told
  Docker users to edit `config.devices.yaml`, which the image carries its own copy
  of, so a credential put there was never read. The guide now says to put the
  blocks in `config.localdb.yaml` next to `compose.yaml` and run `./deploy.sh`,
  which mounts it. Its verify command (`jq '.data[].slug'`) failed on the real
  response, which nests the list under `providers`, and it said to restart, which
  keeps the old mounts. Both editions are fixed. To tell: the guide's steps, run
  on a fresh clone, print `"theta_oura"`.
- **`convert_unit` says why Fahrenheit does not convert.** It refused °F→°C, by
  design (conversions here are a factor), but blamed percentages and molar mass.
  It now says temperature scales also differ by an offset, and gives the formula.
- **The README's badges render on GitHub.** Docker Hub, Downloads and GitHub
  stars showed as broken images, and PyPI did at other times. GitHub serves
  README images through its proxy, camo, which gives up at about 4.5 s. A
  cold fetch through it of shields.io's live badges (PyPI version, Docker
  version, pepy downloads, stars) answered 504 after 4.5 s every time on
  2026-10-01, while badgen and pepy's own badge answered 200 in 0.6–1.0 s.
  Both editions now use badgen and pepy for the live values. Docker Hub
  becomes a static badge naming the image, since it only repeated the PyPI
  version. To tell: the five badges at the top of the README all render.
- **The Data page lists the devices you configured.** With Oura, WHOOP or
  Garmin credentials set, `/api/v1/pulse/providers` answered with the
  device, but the "connect a source" tab said there was nothing to connect.
  Its list was fetched only by the component that renders it, and that
  component was shown only once the list was non-empty, so the request was
  never made. The page now asks for the list itself, and shows a loading
  skeleton until the answer arrives (mirobody-web 61f102e). To tell: set
  `OURA_CLIENT_ID` and `OURA_CLIENT_SECRET` in the config overlay, restart,
  and open Data › connect a source; the Oura row is there.
- **The page no longer fetches provider logos from Theta's servers.** Once a
  device was listed, its row loaded its logo from `static.thetahealth.ai`, so
  a self-hosted page contacted a server outside the deployment. The README's
  privacy line ("nothing leaves your machine except calls to the model you
  chose, and to a device vendor once you link one") did not hold. The web
  client now ships the built-in providers' logos, inlined into the bundle;
  the API's `logo` field is unchanged for other clients. `docs/provider-guide.md`
  had told plugin authors to host their logo on that CDN, and now says the
  field is optional and loaded from wherever it points. To tell: on the tab
  above, every request goes to the stack itself.
- **An upload made after the server restarted is no longer lost.** In a page
  opened before a restart (the README's own order: open the page, add the
  key, `docker compose up -d`), the first file sat at "uploading" forever and
  never reached the server: the page sent it while its socket was still
  connecting and dropped it with a console line. Reproduced 2 of 4 times. The
  upload now waits for the socket to open (10 s), fails the row with a
  message when it cannot, and a tab that is not the leader asks the leader to
  connect (mirobody-web c143e8f). To tell: with the Data page open, restart
  the `mirobody` container and upload; the file is processed.
- **Codex lists and calls the MCP tools.** `/mcp` answered a JSON-RPC
  notification with 200 and a JSON `""` body; Streamable HTTP requires 202
  and no body, and Codex's client re-initialized three times and never
  listed a tool. Every notification now gets 202; any other than
  `notifications/initialized` used to get a "method not found" error.
- **A personal MCP link and a chat share id stay out of the log.** Every
  request line logged its path, and for `/mcp/<secret>` and `/api/share/<id>`
  the path is the credential. That segment is now a digest (`/mcp/~1a2b3c4d`),
  the same for every request of one link. To tell:
  `docker compose logs mirobody | grep '/mcp/'` shows no link.
- **The image run alone says why it cannot start.** `docker run` with no
  Postgres beside it waited on a TCP connect to config.yaml's placeholder
  host with no timeout, logged nothing and stayed `health: starting`. It now
  stops after 10 s, naming the address and `./deploy.sh`.
- **The "no model key" line names the command that works.** It said "and
  restart", and `docker compose restart` keeps the old environment, so the
  key just added was never read. It says `docker compose up -d`.
- **`deploy.sh` stops before Docker does, and says why.** A port another
  program held ended the run in Docker's words ("port is already
  allocated"); a second checkout under the same folder name took over the
  first stack's containers and its database volume without a word. It now
  names the port and the variable to set, and refuses a Compose project name
  another checkout already runs, naming the folder.
- **A genotype file dropped on the Files tab goes to the Genomics tab.**
  There it is checked as one and replacing the active set is confirmed; on
  Files it skipped both, and a VCF was refused as a type the tab does not
  take. The Files hint no longer lists "genetic raw data (txt)".
- **Quieter, truer boot and copy.** Garmin and Whoop without credentials log
  "not configured" at INFO rather than "Failed to create provider" at
  WARNING; the upload-timeout line lost its emoji; `mirobody doctor` prints
  no JSON line above its table. In the page: the tour no longer promises six
  steps over a counter of seven, the medications hint says Data › Records
  like the tab, a VCF's source reads "VCF file" instead of `generic_vcf`, the
  declared build reads 未经核实, and Escape closes the model menu.
- **Docker Hub page synchronization uses the metadata write endpoint.** The
  first sync authenticated but its PATCH to the namespace read endpoint
  returned 403. It now uses the repository write path and current token API;
  `page_only=true` retries metadata without rebuilding an image.
- **Image builds check the installed package before publishing.** Both
  architectures import `mirobody.resolve`, read `BUNDLE_VERSION` and resolve
  hemoglobin outside the source directory, so a stale version stub fails the
  build instead of reaching Docker Hub.
- **A Docker Hub metadata permission error no longer hides a successful image
  publish.** Normal releases warn after the image is available; a strict
  `page_only=true` retry still fails until `DOCKERHUB_TOKEN` has repository
  admin metadata permission.
- **The release workflow uses the supported GitHub Release action runtime.**
  `softprops/action-gh-release@v1` was obsolete and triggered actionlint's
  runtime warning; it now uses the current Node 24-compatible `v3` line.
- **Category words no longer select a specific LOINC assay.** `免疫`, `stool`,
  `重金属`, `heavy metals`, `激素` and `enzymes` now return an explicit
  unresolved result; each names a category or specimen rather than one
  observation. The public skill reference and its shipped regression module
  carry the same rule.
- **The skills evidence module is present in release artifacts.** Wheels now
  include `mirobody/tests/test_skills.py` while checkout-only test modules stay
  out of the install.
- **An erased reading stays erased.** Erasing by document, by name or
  everything removed the readings from the observation model but left their
  1.4 copies in `th_series_data_retired_15`, and the next `mirobody
  migrate-observations` wrote them back. The same erase now marks those
  copies deleted, a moved row is never read again, and merging two accounts
  moves the losing account's unmigrated rows to the one that stays.
- **A reply with no answer text no longer ends the turn empty.** The agent
  asks once more when a reply has neither text nor a tool call; when the
  second is empty too, the turn ends with the "no answer" line and
  `finish_reason=empty`. The request for the answer is not kept in the
  conversation.
- **A model that runs out of calls stops gracefully.** The recursion limit was
  six times the model-call budget for a six-node loop, so any further hook
  turned the budget's graceful stop into `GraphRecursionError` (a model
  repeating one refused call hit it at call 48). Unset, it is now read off
  the built graph; `RECURSION_LIMIT` still overrides.
- **A local model server gets no OpenAI key.** An entry with a `base_url`
  and no `api_key` fell back to `OPENAI_API_KEY`: without one the agent
  loaded no model, and with one it sent that key to the local server.
- **A model that cannot see is not sent images.** A text-only local model
  (MiniCPM5-2B) failed the turn on an attached photo ("image input is not
  supported"). Whether a local model can see is now asked of its server
  (llama.cpp `/props`, without loading anything; Ollama `/api/show`); one that
  cannot gets the photo's OCR text, told that it is only printed text, so a
  meal photo gets "describe it" instead of a guess. An image already in the
  conversation, from before a switch to such a model, is replaced by a line
  saying one was there.
- **The chat's model menu names the model.** A local deployment showed
  `local` where it runs `qwen3.8-27b`; `/api/models?labels=1` gives each
  entry's model, and the bare list is unchanged for other clients.
- **Answers come in the question's language.** The prompt names it when the
  question is mostly in it (Chinese, Traditional Chinese, Japanese, Korean,
  Russian; test names such as LDL or HbA1c do not count); a local model
  answered Chinese questions in English. One Chinese term in an English
  question leaves the answer in English.

## 1.5.3

Postgres is the only state service and deployment is one image; the journal
takes anything, and a medication written in it goes onto the list.

### Added

- **The web client lists every entry, counts what is new, and exports it.**
  The Indicators page could show one indicator's readings at a time, nothing
  said what had arrived since the last visit, and nothing downloaded the
  standardized values. `GET /api/v1/health-indicators/records` pages visible
  entries across all indicators; `GET /api/v1/data/data-delta?since=` counts,
  by source, the entries whose current period began after `since` (a
  correction keeps its entry's period, a retraction ends it, a reassertion
  starts a new one), and `created_since` on the records route lists exactly
  those. `GET /api/v1/health-indicators/export` downloads the same rows as CSV
  or JSON, standardized value and unit included; `GET /api/user/data-export`
  gives the caller's own rows as a JSON page or an NDJSON stream whose footer
  says whether it finished. All four read `PostgresHealthQuery`. To check:
  upload a report, correct one reading, and the delta still counts it once.
- **The journal takes anything; a medication in it goes onto the list.**
  The 记录 box wrote complaints, diagnoses and readings and refused the rest:
  a medication was "log it with your medications" and a meal "not a record".
  A medication part now becomes a `kernel.meds` mention, and
  `reconcile_mentions` decides: a new plan (unconfirmed, labelled as from the
  journal until corrected), a drug already listed left alone, "stopped X"
  stopping the active plan. The model gives the words; the schedule is parsed
  from them, and 每天早晚 now parses as twice a day (it read as once).
  Anything else is kept as an uncoded `note`, so nothing typed is lost. To
  check: log "每天早晚吃二甲双胍500mg，午饭吃了面" and find metformin under
  指标 › 用药 and the noodles in that day's log.
- **Medication plans have an HTTP surface and a tab under 指标.** The model
  had a store and no way in or out of the web. `/api/v1/medications` lists,
  corrects, stops, resumes (opening a new course) and voids plans; each state
  change writes its course in the same transaction, a family member with a
  read grant can list but not write, and a plan the caller may not read
  answers 404 like a missing one. An instruction's own words ("饭后") are kept,
  encrypted, where the structured schedule has no room for them. See
  `docs/medications.md`.
- **`/mirobody.json` names the records, delta and medication surfaces.**
  `__IS_INDICATOR_RECORDS_ON__`, `__IS_INDICATOR_EXPORT_ON__`,
  `__IS_DATA_DELTA_ON__` and `__IS_MEDICATIONS_ON__` follow whether each
  router is mounted, and the bundled client hides what is off.

### Security

- **A personal MCP link outlived the sharing that allowed it.** A link named
  only whose record it read, so when Mom stopped sharing, a family member's
  REST calls were refused (403) while their link kept returning her record for
  up to 30 days. The same gap left links working after removal from the
  circle, after the creator's account was deleted and after a 1.5.2 H1
  take-back. Also: Mom's own link and the one a family member made for her were
  the same URL, so the family member's "revoke" cut off her client; she could
  neither see nor revoke other people's links; and "regenerate" returned the
  first link with its expiry unchanged. Links now live in
  `th_personal_mcp_url` as a hash, one per creator and subject. Every call
  checks expiry, revocation, both accounts, the creator's `tokens_valid_after`
  and, for a family member's link, `resolve_subject`, and refuses with 401 on
  any method. `GET /personal/mcp` lists the links you made and the ones
  reading your record; `DELETE /personal/mcp/{id}` revokes either kind;
  `POST` replaces your link with one valid for 10 days from creation, never
  extended (`MCP_URL_TTL_DAYS`, was 30). Stopping sharing or leaving the
  circle also revokes the family member's link, so sharing again later does
  not bring it back. Settings shows the expiry, regenerate, revoke and who
  holds a link to your record. **Every link made before 1.5.3 stops working**
  (none says who made it): make a new one in Settings. To check: have Mom stop
  sharing, and the family member's link answers 401, before and after she
  shares again.
- **A read-only family member could download a whole record.**
  `GET /api/v1/health-indicators/export` took a care-circle read grant, so a
  member who could page through Mom's rows took all of them, journal notes
  included, in one CSV, while `/api/user/data-export` was already the
  caller's own only. Export is now the owner's on both routes (`403`
  otherwise), and the web client offers it only on your own record. To check:
  export with `target_user_id` set to a member who shares with you.

### Changed

- **A family member with a write grant may change medications, on both ways
  in.** The journal already let a caregiver add, stop and void another
  person's plans ("Dad stopped aspirin"), while `/api/v1/medications` refused
  every write that was not the owner's. Both now follow the grant: write to
  change, read to list. `POST /api/v1/medications?target_user_id=` adds to
  that person's record, `/api/beneficiary-users` says `can_write` per person,
  and the web client shows edit, stop and delete where the server will accept
  them. To check: with a write grant, stop a member's plan under 指标 › 用药.
- **Upgrading a 1.5.2 Compose stack works.** `deploy.sh` stopped for a
  `PG_ENCRYPTION_KEY` that 1.5.2 never wrote (its stacks used `config.yaml`'s
  placeholder); bypassed, it wrote a new `JWT_KEY` over the overlay's and
  signed everyone out, and a 1.5.2 `compose.override.yaml` made compose
  invalid. It now keeps what the old stack used (and the overlay's keys in
  charge), stops with the fix when the override names redis, removes the redis
  container, and names the unused 1.5.2 volumes. It also finds the database of
  a checkout whose directory name has capitals. Uploads 1.5.2 wrote belong to
  root, which this image's user (uid and gid 10001) could read but not add to,
  so every upload failed after an upgrade; a one-shot `mirobody_init` service
  gives the upload volume to that user before each start, which also covers a
  restored 1.5.2 archive. Steps: `docs/backup-restore.md`, "Upgrading from
  1.5.2". To check: upgrade a 1.5.2 stack; a token issued before still works,
  and an upload afterwards succeeds.
- **The one-line install no longer needs the image on Docker Hub.**
  `deploy.sh` falls back to the `docker.1ms.run` mirror when the daemon cannot
  reach Docker Hub (1.5.2 did; 1.5.3 had dropped it), and builds the image
  from the checkout when it cannot be pulled at all, refusing LFS pointers.
  The image now carries the demo seed's documents. To check: run `./deploy.sh`
  on a branch before its release.

- **The application now runs with Postgres as its only state service.** Redis
  previously held login challenges, OAuth state, counters, file cache entries,
  provider locks and worker messages, making a second database mandatory for a
  complete deployment. Expiring values now live encrypted in `th_ephemeral`,
  one-time values are consumed atomically, provider pulls hold session-level
  advisory locks, and the queue uses `th_task_queue`. A clean Compose stack
  reached a healthy server and worker with no Redis package installed; a
  scratch Postgres check exercised expiry, competing claims and lock ownership.
- **Docker deployment now uses a built application image.** The old Compose
  mounted source code and installed dependencies on first boot, so a clone was
  required and startup depended on PyPI. The multi-stage image contains the
  Python app, schema, terminology bundle and current web client; Compose pulls
  it and starts Postgres, server and worker. A local image served `/`,
  `/mirobody.json` and `/api/health` with version `1.5.3.dev0`.
- **Backups now work when a Docker VM cannot bind the destination path.**
  The upload archive previously went into the Docker VM's `/tmp` while the
  script reported success. It now streams to the host, verifies the tar, and
  only then names it as a backup; a Compose test produced readable database
  and upload archives under a host `/tmp` directory.
- **Profile refresh tasks now survive worker interruption.** Redis removed a
  task from its list before it ran, so a stopped worker lost the refresh. The
  Postgres queue claims tasks with a lease and acknowledges only successful
  work; failed work is retried and retained after five failed attempts. A
  scratch database check covered competing workers, retry and acknowledgement.

### Fixed

- **150 concurrent anonymous logins answered 500.** Every temporary-state call
  opened its own Postgres connection, so a burst on the rate-limited routes
  ran out of Postgres's 100 ("too many clients already"). The store now shares
  at most eight, and a limiter that cannot count answers 503, never 500.
  After a Postgres restart the first requests no longer fail either. To check:
  200 concurrent counts hold at most eight connections, and with the cap
  removed the same burst fails with "too many clients already".
- **A journal sentence could fail whole.** An assertion outside the schema
  (`""`, `"affirmed"`) from a provider that does not enforce it was a 500,
  losing the symptoms and readings with it. Any other value is skipped as
  `unclear`, so "没发烧" labelled `absent` is no longer written as a fever. A
  missing one (the schema requires it) reads as present unless the quote
  has a negation word, which is skipped as `unclear` too: a missed entry
  the person can see beats a wrong one they cannot.
- **A task that crashed its worker was claimed forever.** A worker killed
  mid-batch never marked the attempt failed, so the payload came back after
  every lease. A spent task is now failed when its lease runs out, and profile
  generation is bounded by the 600 seconds its lock used to expire after.
- **The in-container check did not run on the image.** `scripts/` is not in
  it and the venv moved, so step 4 of `scripts/e2e_docker.sh` failed and step
  5 could pass having read no log. The script is piped in now
  (`docker compose exec -T mirobody python - --user 1 < scripts/e2e_health_data.py`),
  and a missing container fails the check.
- **Smaller:** `shell/backup.sh` writes its files 0600 and streams the dump
  out (snap Docker refused `/tmp`); `deploy.sh` prints the port `.env` sets,
  and finds the image by service, where an image without "mirobody" in its
  name ended the script silently;
  "每天两次…每天三次" is no parse instead of the first count, and "早、晚" and
  "as required" are read; voiding a plan closes its open course; the NDJSON
  export says `complete: false` when the record changed while it streamed;
  two personal links made at once for one pair no longer collide.

## 1.5.2

Genotype uploads arrive as facts: one call per site, checked against a bundled
public site index, with CPIC drug-gene coverage on top. Mirobody names no drug
phenotype and no rare-disease risk from an array: the coverage tool reports
`not_determined` rather than guess from calls that cannot establish one.
[docs/genetics.md](docs/genetics.md) states the scope and the reason for each
boundary. Around it, the findings of a fresh-deploy review: MCP signs in the
way its specification says, a token works only where it was issued for, an
address is proven before it is held, and a family member you keep a record
for can take it over.

### Added

- **A virtual member can take over their own account.** The person who added
  them gets a one-time link (`POST /account/activation`, valid 7 days) from the
  member's card. Whoever opens it proves the address it names, by a code sent
  there or, where the deployment sends no mail, a password, and the account
  and all its data become theirs; they choose what the creator keeps (edit,
  view or nothing). An address that already has an account receives the
  record by merge.
- **Complaints and diagnoses resolve in Japanese, Russian and Traditional
  Chinese.** LOINC names already resolved in English, 简体中文, 繁體中文,
  日本語 and Russian (#88); ICPC-3 complaints resolved only in Chinese and
  English, and 繁體中文 not at all (發燒, 頭痛, 高血壓 all `no-match`), because
  the complaint axis never applied the zh-Hant fold the LOINC side does. It now
  does, term as written first, and Taiwan's 氣喘 (asthma) is curated under its
  own spelling. `symptoms_ja.tsv`, `conditions_ja.tsv`, `symptoms_ru.tsv` and
  `conditions_ru.tsv` are new, our own patient phrasing on the codes the
  Chinese and English files already use, with the same `!too-broad` /
  `!ambiguous` sentinels (痛み, боль; 糖尿病, сахарный диабет). The
  health-records benchmark now carries cases in all five and fails if a
  language drops out: `python -m unittest benchmarks.health_records.test_cases`.
- **Every response names its request id, and a failure quotes it.** The id
  that ties a request's log lines together was minted only for signed-in
  requests and never left the server, so "it said internal error" matched
  every error that day. Every request now gets one (the caller's
  `X-Request-Id` or `X-Trace-Id` when it is 1-64 of `[A-Za-z0-9._:-]`, else a
  fresh uuid), every response returns it as `X-Request-Id` (exposed to browser
  scripts), and the chat's internal-error message ends with
  `(reference: <id>)`. Grep the logs for that id.
- **The genetics page has a Chinese edition.** The Chinese README linked the
  genetics page in English only, although every other guide it links has a
  `<guide>.zh-CN.md`. `docs/genetics.zh-CN.md` now pairs with
  `docs/genetics.md`, and the Chinese README and the docs index point to it.
  The file-processing table also listed four vendors and a single-member ZIP
  for genotype uploads; it now names FTDNA, BGZF and a ZIP of one VCF with
  BED/TXT sidecars, as the handler accepts.
- **Phone health-store records now use device provenance.** `POST /api/data`
  resolves recognized vendor identifiers through the shipped crosswalk and
  keeps ambiguous fields uncoded. Mixed sources retain one atomic ingest and
  the response's rejected counts; submit a Health Connect heart-rate identifier
  and inspect its LOINC code and device source on readback.
- **Headless clients can read the API capability version.** `/mirobody.json`
  now serves `capability_version` and `server_version` even without a web
  client. A GET or HEAD request verifies that the route exists.
- **The offline device vocabulary can now be exported for phone clients.**
  `mirobody device-bundle [--out PATH]` writes the metric catalogue, labels,
  LOINC crosswalk and vendor fields with a canonical SHA-256 digest. Run the
  command twice and compare the bytes to check reproducibility.
- **Tracked vocabulary examples for indicators and complaints.** The earlier
  cases lived only in ignored local tests, so a fresh clone could not replay
  typical LOINC/UCUM and ICPC-3 outcomes. `benchmarks/health_records/` now
  holds a small synthetic case set and a command that checks the public APIs.
- **Small public genotype examples now ship for external parser tests.** The
  previous public truth lived only in ignored maintainer corpora, so a wheel
  user could not reproduce cross-format normalization. Thirteen pinned 1000
  Genomes HG00096 calls across twelve genes now appear as five vendor-shaped
  exports plus both genome builds, gzip/BGZF and ZIP renderings under
  `mirobody/testing/genomics/`. A separate public no-call and female X call
  exercise conservative statuses. `canonical.json` and `manifest.json` pin
  expected results and source hashes; the tracked benchmark test checks them
  without downloading a whole genome.
- **Genotype uploads now publish an atomic active set.** A failed batch used to
  leave partial data while still reporting completion, and a repeat upload
  duplicated rows. The new set remains hidden until every batch and row count
  pass; another upload replaces it. The public 1000 Genomes end-to-end check
  uploads nine public renderings, including gzip/BGZF/zip and both genome builds,
  and observes one active set after each replacement.
- **Genotype calls are checked against a bundled public site index.** 1.5.1
  stored each row as uploaded, with no reference build, REF/ALT or strand
  check. A SHA-256-pinned dbSNP 155 Common extract now maps 489 SNVs across twelve
  pharmacogene regions in both GRCh37 and GRCh38; the public two-site upload
  and Agent path uses the packaged asset. One PGP 23andMe v5 export has
  262/625,705 observed rsIDs covered and one PGP AncestryDNA v2 export has
  340/677,436. This is limited regional coverage, not a full chip or rare
  variant catalogue; unmatched rows remain `unresolved`.
- **Normalized genotype queries, VCF export and a bounded CPIC coverage tool.**
  The old genetic tool could only read rsIDs. It now offers an overview, gene
  and build-specific region search; authenticated users can export mapped
  GRCh37/38 VCF, and the Agent/MCP can check CPIC A/B drug-gene links without
  inventing a phenotype. The public two-site integration test checks upload,
  MCP, VCF and real Agent answers. The site index covers twelve regions, so a
  call outside them stays `unresolved` and is still found by rsID.
- **Genotype uploads are no longer readable through the Agent's document mounts.**
  The file projection previously exposed raw genetic exports through
  `/uploads/` or `/library/`, bypassing the bounded genetic tool. Both mounts
  now exclude genetic files; a public-upload database check exercises each
  projection. The CPIC tool also names missing and no-call definition rsIDs,
  allowing an answer to cite the actual coverage gap instead of only counts.
- **VCF export now carries upload provenance and natural contig order.**
  Lexical chromosome ordering could put 10 before 2, which downstream VCF
  tools may reject. The exporter walks each indexed chromosome in numeric
  order and includes the format, vendor and normalization versions in its
  header; the public upload round trip checks both.
- **Genotype batches now insert as column arrays.** The per-row executemany
  path took 80.254 s for 1.3 million public GIAB HG005 SNVs, over the 60 s
  budget for a whole-chip import. One SQL insert per 50,000-row batch took
  42.074 s on the same isolated PostgreSQL host; both runs activated all
  1.3 million calls. The benchmark script pins the public source SHA.
- **The Genomics Data page now ships in the bundled web client.** The page has
  a dedicated upload entry, processing state and active-set summary. Its source
  passed 167 tests, lint with zero errors and the open-source build; the local
  backend serves the copied hashed assets. The page accepts `.bgz/.bgzf`,
  describes ZIP sidecars in all four languages and uses a stable completion
  message. A Chrome check saw the active-set card update after a public-data
  replacement, and a public VCF dispatched through the page's file-input
  handler activated a two-row set.
- **Mapped genetic calls have a bounded FHIR Variant export.** The authenticated
  `export.fhir.json` route represents selected rsIDs with the STU3 assessment,
  reference assembly, coordinate, REF/ALT and allelic-state codes. Missing and
  unrepresentable rows are named rather than silently treated as normal. All
  nine public upload formats reproduce the two-site truth in this export.
- **`mirobody migrate-genotypes` for 1.5.1 data.** The new reader only sees
  active sets, which otherwise hid legacy rows on upgrade. The command moves
  each person's latest old file into an active set with raw calls marked
  unverified; a public-data migration check verifies both visibility and safe
  reruns. Re-upload the source to obtain verified GTs.
- **Pinned CPIC releases can be installed and selected without executing SQL.**
  Previously the coverage tool could only read the bundled v1.60.0 extract.
  `mirobody fetch cpic --version vX.Y.Z` now validates the public dump's COPY
  data and installs an immutable extract; `CPIC_VERSION` selects an exact or
  locally newest installed version at query time. Public v1.59.1 and v1.60.0
  dumps passed the offline fetch and selection checks.

### Security

- **The webhook routes wrote into any account, unauthenticated.**
  `POST /api/v1/pulse/{platform}/webhook` and `/{platform}/{provider}/webhook`
  took no credential, and the Apple platform reads the account from `user_id`
  in the body: one anonymous POST added a medication to another user's plan.
  They now answer 404 unless `COLLECT_WEBHOOK_SECRET` is set, require it
  (`X-Webhook-Secret` or `?secret=`) when it is, never reach the Apple
  platform, and drop `theta_user_id` / `app_user_id` from the payload.
- **A genotype file without its header went to the extraction model.** The
  classifier keyed on the column header, so a 23andMe export with its comment
  lines removed took the document path and its calls reached two model
  requests. Rows shaped like calls, and any `.vcf` / `.vcf.gz` / `.vcf.bgz` by
  name, now take the genetic path on upload, in chat and in the agent's file
  mounts; the reader refuses them with a request for the original export.
- **The shipped placeholder `JWT_KEY` signed tokens on the network.** The
  placeholder check ran only under `PRODUCTION: true` and the default bind is
  `0.0.0.0`, so `mirobody serve` from a clone accepted tokens forged from the
  public string. Off loopback the run now gets its own key and says so.
  `deploy.sh` draws its generated keys from `/dev/urandom`, not `$RANDOM`.
- **`/files` checked who uploaded a file, not whose record it is.** A report
  a carer filed for someone could not be opened by that person, and anyone
  allowed into the carer's record could open it. Access now follows
  `query_user_id`, the column the file list already filters on.
- **A virtual member's address came from the client.** `POST /api/user/virtual`
  stored whatever address it was sent, and the account sat read-write in the
  caller's circle, so a real address put that person's future sign-ins
  inside it. The server now mints an undeliverable `@virtual.invalid`
  address, marks the account `managed_by` its creator, and never signs anyone
  in to it.
- **Anyone who knew a session id could read and write that conversation.**
  The agent's checkpoint thread was the client-supplied `session_id` alone, so
  a second account posting a known id resumed the first account's turns (the
  model repeated its LDL and HDL back) and its own message became part of the
  owner's next turn. Threads are now `<owner>:<session>`
  (`agent/checkpointer.py::thread_for`); `90_retire.sql` re-keys existing
  threads under their first message's sender. To check, post someone else's
  session id: the answer says it is the first message.
- **`POST /password/register` took over any account without a password.**
  Every account made by email code, Apple or Google has no password hash, and
  registering its email set one and returned that account's tokens. It now
  creates new accounts only; an existing email answers -4.
- **`POST /api/v1/pulse/{platform}/token` issued a 30-day token without
  checking anything.** The provider's `_validate_credentials` was its only
  proof, and the default accepts everything, which every shipped provider
  inherits. A provider that does not override it now answers 401.

- **A passkey protected nothing.** An account with MFA on signs in with an
  AAL1 fallback token, and no route ever asked for more. Every request of such
  an account now needs `aal` >= 2, except the WebAuthn and session routes that
  raise it and the settings read the web client makes first; the client
  already answers `403 ERROR_AAL2_REQUIRED` by running the passkey upgrade and
  retrying. Turning MFA off, minting an MCP URL and `/mcp` are gated too. MCP
  clients an MFA account authorised before this carry no `aal` and must be
  authorised again.
- **The assurance level no longer launders through a refresh.** A refresh
  token was accepted as a bearer credential (60 days), and `/oauth/token`
  accepted ANY valid token as a refresh token, returning a fresh 30-day token
  with no `aal`. Refresh tokens are now refused as bearers, the token endpoint
  requires a refresh token issued to the presenting client, and `aal` rides
  from the authorising session through the code, the tokens and every refresh.
  `/mcp`, `/personal/mcp`, the chat service and the WebAuthn routes decoded
  the header a second time and took a refresh token too; they no longer do.
- **An MCP client's token worked on every REST route.** A connector granted
  `mcp:read` could call `DELETE /api/data?all=true`. Tokens from
  `/oauth/token` now name this server's MCP endpoint as their audience
  (RFC 8707), are accepted only there, and cannot approve another client.
  Connectors authorised before 1.5.2 get a 401 and refresh.
- **The out-of-band MCP sign-in handed out the approver's web session.**
  An unauthenticated `tools/call` returned a sign-in link with a `state` the
  caller chose, and `/oauth2/check_state/{state}` then returned the web token
  of whoever approved it, to anyone, repeatedly. Both are gone, with the
  never-working `credentials` grant. `tools/call` without a valid token now
  answers 401 with `WWW-Authenticate` naming
  `/.well-known/oauth-protected-resource/mcp` (RFC 9728), and OAuth with PKCE
  takes over.
- **Registering an address did not prove it.** A password registration for
  someone's address became the account their later code sign-in landed in,
  with the claimant's password and invitations still live. Where mail is
  configured, `/password/register` now needs the code sent to the address
  (`__IS_SIGNUP_CODE_ON__` in `/mirobody.json`). A code sign-in to an account
  whose password was never proven clears that password and ends every earlier
  session (`tokens_valid_after`), and such an account cannot accept an
  invitation until then.
- **OAuth: PKCE and `redirect_uri` are checked.** The metadata advertised
  S256 and nothing verified it. A redirect flow now requires
  `code_challenge_method=S256`, and the token request must present the
  matching `code_verifier` and the same `redirect_uri`. A signed-in
  `GET /oauth/authorize` no longer puts the session token in the redirect URL.
- **Chat attachments are checked for ownership.** A request named its files by
  key and nothing asked whose they were: a key taken from someone else's
  shared conversation was downloaded, extracted into the caller's record, and
  its row rewritten. Keys another account holds are now dropped before the
  fetch, and the upsert touches only the uploader's own row. Attachment bytes
  are no longer copied into Redis (base64, an hour, keyed by file key alone).
- **Sign in with Apple and Google is removed.** Neither the web client nor
  anything in this repository called `/apple/verify` or `/google/verify`, and
  the Apple path checked no audience, so an id_token issued to any app signed
  its holder in here and matched their account by email. `APPLE_*` and
  `GOOGLE_CLIENT_ID` are no longer read, `/mirobody.json` drops its two sign-in
  flags, and `health_app_user.apple_sub` is kept but no longer written. Sign-in
  is by email code or password. `ensure_user` is now the one find-or-create by
  email (it was two, one of which raced).

### Changed

- **The genetic tools say what they could not match.** `query_pharmacogenomics`
  dropped an unknown drug silently when another matched
  (`["clopidogrel", "warfarin", "notadrug"]` said nothing about the third); it
  now notes `no CPIC A/B gene-drug pair for: notadrug`. `query_genetic_data`
  refused `chr10` and `chrM`; they are read as `10` and `MT`.
- **`convert_unit` no longer reports Infinity or NaN as a conversion.**
  `1e308 g` to `ug` answered `success: true, converted: Infinity`; a
  non-finite value or result is now refused with a reason.
- **An upload's results no longer race its own row.** The Data page upload
  inserts `th_files` rows after the whole batch, while indicator extraction
  and genotype processing wrote to their row as soon as they finished; one
  that finished first matched no row (`File not found for update`), and the
  report date, readings count or final status were lost. They now wait for
  the batch insert.
- **A multi-file upload session filed every file twice.** Processing started
  once the files that had begun to arrive were complete, which in a two-file
  batch was after the first file and again after the second. It now starts
  once, after every file `upload_start` declared. The web client sends one
  file per session and was not affected.
- **An upload with no abstract from its handler lost its generated name.**
  The fallback abstract extractor called its text helper with two arguments
  it did not take; the `TypeError` came before any model call. PDFs and
  images now get their name and abstract on that path too.
- **Merging two accounts orphaned the losing one's data.** `/email/bind`
  merges an account into the one that already holds the address. Its table
  check asked for `public.<table>` while the shipped config puts the tables in
  `theta_ai`, so nothing moved and the losing account was still closed.
  Medications were not on the list either. Both are fixed, checked against a
  real Postgres.
- **Asking on someone's behalf answered as if the record were the asker's.**
  The agent read the other person's record but was never told so. Answers said
  "your cholesterol" over her numbers and flagged `mom_lab_2025-11.md` as "not
  yours". Asked "妈妈的胆固醇", the model guessed `member="妈妈"`, the circle
  check refused it, and a person who could read the record was told they could
  not. The chat layer now passes the record owner's name (`record_owner`) and
  the prompt says whose record it is; it no longer carries the raw `user_id`.
  To check, switch to Mom and ask 「妈妈的胆固醇这几次是怎么变化的？」: the
  answer reads her record and no tool call is refused.
- **No tool takes `member` any more, on either surface (breaking for MCP
  clients).** Whose data a call reads is its authentication: the MCP token or
  URL, or the record a chat turn was opened on and authorised for. One call,
  one person. A `member` argument is refused as an unknown parameter.
- **`query_health_indicators` has five parameters: `keywords`, `indicators`,
  `start`, `end`, `view` (breaking for MCP clients).** `resolution` ×
  `aggregate` was eighteen cells with two refused, and `limit` applied to one;
  `view` is one enum, `raw | minute | hour | day | week | month | stats |
  latest`. Raw rows are capped at 50 per indicator (`query.ROW_CAP`). What
  `stats` counts is no longer the caller's choice: per series and local day,
  the elected authority where one was published, every reading otherwise. The
  old `resolution=raw` basis averaged a watch's and a phone's totals for one
  day together; the old `day` basis dropped a morning blood pressure followed
  by an evening one. The REST route takes `view` too; `resolution`,
  `aggregate` and `limit` are ignored there, and the browser's reading list is
  capped by `collect.REST_ROW_MAX` (200, what it asked for). Ask for `view="stats"` and the meta
  line reads `view=stats`.
- **`query_genetic_data` dropped `include_nearby`, `nearby_range` and `limit`
  (breaking for MCP clients).** "Nearby" was physical distance, which is not
  linkage: a proxy for an untyped site needs an LD reference panel, and the
  code only looked around sites that WERE typed, where no proxy is needed.
  What remained is a region query, which `chromosome`/`start`/`end` already
  are. Direct rows are capped at 100 (`ROW_CAP`); a gene selects at most seven.
- **MCP clients got `isError: false` on a failed record-tool call.** The
  record tools report `status`, never `success`, so a refused or failed read
  arrived as a successful result whose text began "error (". `status: error`
  now sets `isError`.
- **`GET /mcp` answered a JSON-RPC parse error.** It now answers 405 with
  `Allow: POST, OPTIONS`, as Streamable HTTP specifies for a server with no
  event stream.
- **A turn on someone else's record names them the way the asker does.**
  `record_owner` is the care circle's label ("妈妈") before the account name.
- **Deleting every file of a message deleted nothing.** It looked the files
  up by `created_source="file_upload"`, a value nothing writes, found none, and
  answered success while the rows, the stored objects and their readings
  stayed. It now finds them by the message id within the caller's files.
  `th_files.created_source` is now `data` or `ask`, after the web client's
  tabs (it was `web_drive`/`web_chat`); `90_retire.sql` renames existing rows.
- **Parameters and state nothing read are gone.** `POST /api/chat` no longer
  accepts `user_name`, `token` or `trace_id` in its body (none was read; the
  request id travels in `X-Request-Id`). A tool's injected `user_info` is
  `{"user_id"}` only, on both surfaces: `token`, `session_id` and `success`
  reached no tool. Removed with no caller: `collect/core/database.py` (its one
  live path, the provider list's record counts, always returned nothing), the
  provider webhook-management methods (`get_webhooks`, `check_format`,
  `sync_user_devices`, the raw-data readers), `mcp.call_global_tool`,
  `get_global_tool_count`, `reset_global_tools`, and the agent's
  `FILE_CACHE_TTL`/`FILE_CACHE_MAXSIZE` reads. The prompt no longer renders
  `user_name` (always "User"), `language` or `user_info`; a deployment's own
  template naming them must drop them.
- **Every upload left a copy of the document in `/tmp`.** Each file handler
  saved the upload to a temporary file that only the text handler reads and
  nothing deleted: eight lab reports in the demo container. The copy is now
  deleted as soon as the handler returns. Upload a file and `ls /tmp`.
- **`DISALLOWED_TOOLS: [eval]` turns the REPL off**, as the agent README said
  it did; it only ever filtered the MCP tool list.
- **MCP no longer advertises `prompts`.** It answered `prompts/list` with an
  empty list; now neither is offered and the method is not found, as for
  resources. `/api/health` counts tools through `McpService.tool_counts()`.
- **The embedding surface is gone.** Nothing had embedded since 1.5.0 deleted
  the semantic tier, but `mirobody/utils/embedding.py`, `LLMConfig`,
  `Config.get_llm`, the `UTILS_EMBEDDING_MODEL` route, four `*-embed` MODELS
  entries and a doctor row outlived it, and every boot of a DeepSeek- or
  Anthropic-only deployment warned that a feature that does not exist had no
  model. A config still setting `UTILS_EMBEDDING_MODEL` or
  `EMBEDDING_PROVIDER` is told at boot that nothing reads it, and a MODELS
  entry still carrying `embedding:` is named by the unread-key check.
- **`query_genetic_data` stopped refusing placeholder coordinates.** Some models
  fill every schema field, sending `chromosome: ""`, `start: 1`, `end: 1`,
  `build: "GRCh38"` beside `gene`; any of those counted as a region, so the call
  was refused as two selectors and retried in the same shape (gpt: 4-6 refusals
  a turn). A region is now asked for by naming a chromosome; beside rsIDs or a
  gene the coordinates are ignored, and coordinates alone are refused for want
  of a chromosome. Ask gpt "我的 CYP2C19 基因型是什么？": one call, no refusal.
- **The system prompt is a third shorter, measured to behave the same.** The
  template went from 13,537 to 9,141 characters: tool-specific caveats that
  already ride on every tool result as `notes:` left, the depth, chart, table
  and lab-report guidance was compressed, and maintainer rationale moved into
  Jinja comments, which are not sent. Measured on 8 questions x 3 models x 2
  repetitions against the seeded stack: 48/48 before and after, tool calls per
  turn 1.44/3.44/1.56 before and 1.31/3.50/1.56 after (claude-sonnet / qwen /
  gpt). The `# Available tools` section stays although the same descriptions
  travel as tool definitions: removing it made qwen repeat identical calls (0 to
  5-8 per 16 turns) and call 24-34% more tools; claude and gpt did not change.
- **The chat agent no longer sees `resolve_indicator`, `convert_unit` or
  `normalize_unit`.** `query_health_indicators` already resolves names to
  LOINC and values to the catalogue's unit; those three serve an MCP client
  holding readings of its own, and stay there.
- Focused health trends could send models toward summary queries without chart
  points, repeat lookups after enough data had arrived, or mix unlike units on
  one chart axis. The Agent prompt now gives valid point, summary and latest
  query shapes; bounds retries; uses separately labeled charts for unlike
  units; and avoids calling a value normal when the report's range is absent.
  `benchmarks/local_agent/compare_mimo_bonsai.py` replays the mixed-unit and
  same-unit chart questions, among six synthetic cases, against any
  OpenAI-compatible model server and records each tool call and reply.
- The 40-question Agent check previously counted generic "no question" replies
  as answered. It now marks them invalid. On pinned public two-site truth,
  Qwen selected the expected tool and gave a valid first answer in 37/40
  questions, with zero automated forbidden-claim alarms; targeted reruns
  recovered the three misses across configured Qwen and OpenRouter GPT.
- Some public whole-genome VCF uploads were not recognized: BGZF has multiple
  gzip blocks, a Big-Y archive ships one VCF with BED/TXT sidecars, and one
  WGS header exceeded the 16 KiB sniff window. The parser now validates each
  BGZF block, permits one VCF with bounded and CRC-checked sidecars, refuses a
  second genotype-looking sidecar, and reads
  up to 256 KiB of header. Chat attachment classification and legacy file
  mounts use the same bound, so a long-header VCF or `.bgz` cannot appear as
  a document. All 17 accessible public PGP genotype exports classify; the
  public HTML report remains rejected. All 17 original public PGP genotype
  exports activated through isolated WebSocket/PostgreSQL checks, including
  two BGZF WGS files with 4,741,304 and 5,017,551 rows. Their VCF GTs are
  self-described and do not assert whole-genome catalog coverage.
- The public Big-Y ZIP reached the database but did not fit a three-character
  chromosome or a 100-character REF/ALT column: two alternate contig names and
  four ALT strings are longer. The genotype table now stores those raw fields
  as text, and schema replay widens a table created with the narrower columns.
  The public 444,297-row
  Big-Y upload activates and remains queryable. A failed driver statement used
  to log a traceback that could quote bound genotype values; the central SQL
  writer now logs only the error type and counts for driver exceptions.
- Public Ancestry PAR rows were stored but the region tool refused `PAR` and
  required a reference build they did not have. `query_genetic_data` now
  accepts an explicit `build=raw` region, labels its unverified coordinates,
  and indexes raw positions. The public Ancestry v2 export has 27,206
  X/Y/PAR/MT rows; a bounded PAR lookup is in the public end-to-end check.
  The added raw-coordinate index kept a fresh 1.3-million-row public GIAB
  import at 42.510 s, within the 60 s budget; table and indexes grew by
  280,338,432 bytes in the isolated audit schema.
- A public VCF that listed only one ALT allele was previously rejected when
  the dbSNP site listed additional alleles. VCF GT indexes now map into the
  catalog's allele order while preserving phase. The public HG00096
  rs4244285 call and all nine upload renderings pass with the packaged
  multi-allelic site index.
- Chat attachments previously classified a valid compressed genotype file from
  a truncated archive prefix and exposed its raw bytes in the Agent file
  mounts. Each attachment now gets its own scene after complete gzip/zip
  validation; the public privacy check inserts plain, gzip and zip uploads
  and confirms both mounts hide them.
- Region queries filtered by the requested assembly but could display the
  upload's other assembly coordinate as `position`. Results now return the
  requested coordinate plus `query_build`, `raw_position`, `pos37` and `pos38`;
  public GRCh37 and GRCh38 VCF uploads pass cross-build query checks.
- Sex inference used to leave heterozygous non-PAR X/Y calls counted as valid.
  Activation now marks conflicts unresolved, preserves diploid PAR calls and
  recounts `n_called` after correction; pinned public 1000G X calls pass the
  PostgreSQL activation check, including an unknown-build case.
- DeepAgents' separate conversation summarizer could receive old genotype
  tool rows and write them to a readable history file before the ordinary
  model-call guard ran. The summarization slot now redacts genotype results
  and dependent answers before summary or history writes, and overflow
  recovery clips only the redacted view. After a genotype query the Agent also
  refuses scratch-file writes and reads, while the document mounts remain
  readable. Public-call tests exercise synchronous and asynchronous paths;
  a two-question live replay now verifies three model boundaries with the
  prior genotype row and answer redacted.
- Previously mislabeled 1.5.1 genetic attachments could remain in the Agent
  document mounts even after new uploads were classified correctly. The read
  projection now hides files linked to genotype sets, plain exports with a
  genotype header, and legacy gzip/zip containers; a final content check
  refuses raw genetic bytes when cached text is absent. The public PostgreSQL
  privacy check inserts mislabeled rows and confirms both mounts hide them.
- Genetic resolver terms previously missed specific CYP2C19, MTHFR, APOE and
  HLA-B LOINC concepts or resolved ambiguous phrases as a genotype. Nine
  explicit mappings now target the named concepts and five broad phrases are
  rejected; the resolver override table records each term and target.
- Removed the frozen Traditional Chinese and Japanese README editions and their
  archive index, which contained stale links. Only the live English and Chinese
  READMEs remain; the removed editions are available in Git history.
- **A quickstart path could fail before the server started.** The checkout guide now includes clone and database setup before `mirobody dev`, the example Postgres user matches the connection URL, and the CLI's missing-database hint uses the same credentials. The Docker guide runs `doctor` in its container, and the hosted link opens the separate Cloud quickstart. Follow either path and check `/api/health` to confirm the local server is running.

## 1.5.1

The symptom axis: complaints and diagnoses in a person's own words, coded on
ICPC-3, a journal that takes a whole sentence, and an agent that reads them
beside the readings. Around it, UCUM ships with the tables that implement it,
an MCP server runs on a bare `pip install mirobody`, and deleting an account
now deletes it and ends its sessions.

### Added

- **An MCP server that runs on `pip install mirobody` alone.** `mirobody mcp`
  (or `uvx --from mirobody mirobody-mcp`) serves the shipped vocabularies over
  stdio: no database, no key, and nothing but the library and numpy loaded.
  `standardize_reading` answers a reading as printed with a FHIR Observation
  carrying its LOINC code and UCUM unit (`血红蛋白 13.5 g/dL` gives 718-7), and
  points a complaint (发烧) at `standardize_complaint` rather than code it;
  how it decided rides in a `coding-decision` extension, so the resource
  passes strict R4 validation. `standardize_complaint` codes a complaint or
  diagnosis on ICPC-3, and
  `standardize_report` a whole report when `[parse]` and a model key are
  there. Four prompts and two resources (the vocabulary releases and their
  notices, the indicator catalogue). Checked with the official MCP SDK's own
  client. `server.json` is the MCP Registry entry; publishing it is a
  separate step.
- **`mirobody.standardize_reading`**, the same Observation from the library.

- **Two ICPC-3 axes.** `mirobody.translate.resolve_symptom()` turns a
  complaint in a person's own words into one ICPC-3 S code, and
  `resolve_condition()` turns a named diagnosis into one D code. Each abstains
  with a reason rather than guessing, on the same contract as `code()`: three
  outcomes, a decision id, no guess. They are separate indexes and the caller
  picks, so 发烧 answers only as a complaint and 高血压 only as a diagnosis.
  The vocabulary ships verbatim under CC BY-ND, 1,218 codes in `res/icpc3/icpc3.tsv`
  with its NOTICE; the everyday Chinese and English spellings that reach it are
  ours, Apache-2.0, and hold no ICPC-3 term in any language, because WONCA
  licenses translations of the electronic version separately. Display names are
  ICPC-3's English. A coding names `icpc-3+<digest>`, a stamp over the shipped
  terms and our surfaces, because ICPC-3 publishes no release number in the
  data we hold.
  The classification's index words are deliberately not shipped: 1,273 of
  3,139 of them (40.6%) are character-identical to a SNOMED CT description on
  the same code, and `res/` carries no SNOMED CT derivative.

- **`/api/v1/journal`: log what a person reports, read it back by day.** POST
  one entry with `kind` of `symptom` or `condition`, GET the log grouped by the
  day it was felt, DELETE one. The day is the writer's (`tz`, else the
  `X-Timezone` header; a `tz` no zone answers to is refused), and an entry
  deleted can be logged again; a device
  re-sync does not bring a deleted reading back. No new table: both are one
  self-reported observation, so they inherit the append-only history, the day
  placement and the coding trail. The list gives both names, the person's words
  and the classification's, and keeps the entries the vocabulary could not
  place with the reason attached. The agent and MCP clients read them through
  `query_health_indicators`, as a table of their own beside the readings, so
  "was my blood pressure up on the days I had headaches" is one call; the web
  client's Indicators tab still lists readings only.

- **One sentence, every entry it states.** `POST /api/v1/journal/sentence`
  takes what a person typed ("我头疼，血压150/95，没发烧") and writes a
  headache, a systolic and a diastolic reading. A model splits and types the
  parts and is never asked for a code: each part is coded at write time on its
  own axis, LOINC for 收缩压, ICPC-3 for 头疼. A part the sentence does not
  quote, a negation, a guess, someone else's condition and a medication are
  not written and come back with the reason. Writing into someone else's
  record, the model is told whose it is, so 我爸咳嗽 in Dad's record is his. A
  typed reading is held to the ingestion range a device reading of the same
  code is (血压 400/300 is refused). On twelve real sentences
  (gemini-3.8-flash) every part was split and typed as expected. On 160
  symptoms split from real consultation texts, 22 were a fragment without its
  body site (脱落 for hair loss) or no complaint at all (效果明显); the prompt
  now keeps the site and drops improvements, which fixed 16 of the 22.
  Readings typed this way are listed in the journal and are readings
  everywhere else.

- **Body water and bone percentage carry codes.** `bodyWater` was in the
  catalogue uncoded since 1.4.0 and now carries `101684-9`; `bonePercentage`
  is new, `101686-4`, with an ingestion range. An impedance scale reports the
  mass and the percentage, and they differ in PROPERTY, so they are four rows.
  The catalogue is 316 metrics over 307 names, and `TERMINOLOGY_VERSION` is
  `1.5.1`.

- Offline resolver: Russian panel terms as real Russian lab reports print
  them (МЕДСИ, INVITRO, state clinics; ~80 spellings from a 2019-2026 record
  archive), plus the three Estonian vitamin spellings a Synlab report prints.
  CBC, biochemistry, thyroid hormones and vitamins now resolve to definite
  LOINC codes — СОЭ to 30341-2, ТТГ to 3016-3, Билирубин прямой to
  Bilirubin.direct, Витамин D (25-OH) to the D2+D3 sum code — and the
  coverage benchmark carries the panel with must-not traps (direct vs
  indirect bilirubin, the neutrophil fraction vs the absolute count, the
  thyroglobulin antibody vs the antigen).
- The cross-language identity tests ship with a clone
  (`mirobody/tests/test_cross_language_identity.py`): every spelling of an
  analyte, in every language, must answer one code. The 13 analytes that do not
  agree yet, among them `urea` landing on urea nitrogen and the Russian
  differential counts, are strict xfails, each an open fix.

### Changed

- **The symptom axis, measured on real complaints (E0).** 200 complaints
  split from real online-consultation texts (ChatMed, CC BY 4.0; English from
  symptom_to_diagnosis), each labelled by three independent model annotators
  blind to the resolver (Fleiss' kappa 0.84). On the 142 scorable symptoms the
  resolver gave no wrong code (wrong-rate 0.000; the gate is 0.03) and coded
  29, one in five: precise, and far from covering how people write. The
  labels are awaiting a human review.

- **UCUM ships with the tables that implement it.** `res/ucum/ucum-essence.xml`
  is UCUM 2.2 byte for byte, with its notice and licence; an edited copy is
  refused. A gate now holds our unit tables to it, and its first run found
  five canonical units UCUM does not define: `/HPF` and `/LPF` (UCUM and
  LOINC print `/[HPF]`, `/[LPF]`, so ours matched none of LOINC's 249 example
  units), `osmol` (UCUM's is `osm`) and `k[arb'U]/mL`. The table is 328 units,
  not 331; `k[arb'U]/L` stays because LOINC prints it. Every conversion
  factor agreed with UCUM's definitions.

- **`mirobody/res/` groups by vocabulary.** The bundle and everything that
  steers it are under `res/loinc/`, the ICPC-3 table and our surfaces onto it
  under `res/icpc3/`, the indicator catalogue and its labels under
  `res/catalog/`; `crosswalks/` is unchanged, and `dose_forms.tsv` and
  `EXTERNAL.tsv` stay at the top because they belong to no vocabulary. Against
  1.5.0 the top level went from seven files and three directories to three and
  five, and `res/README.md` now says what each file is and who opens it.
  `mirobody.bundle.BUNDLE_PATH`, `RES_DIR` and `ALIAS_SRC_DIR` are computed and
  keep working; code that hardcoded `res/fhir_loinc_bundle.tar.gz` does not.
  The Git LFS patterns in `.gitattributes` are `res/**/*.gz` now, not
  `res/*.gz`: a pattern that stops matching checks out the pointer text in
  place of the data, which presents as a corrupt bundle rather than a wrong
  path.
- **`HRV` and `心率变异性` resolve to `112429-6`, not `76643-6`.** LOINC has no
  code for heart rate variability with the algorithm unstated, so a bare "HRV"
  asserts SDNN whichever of the three is chosen; `112429-6` is the only one
  that does not also assert `SYSTEM=Heart` and, for `76643-6`, `METHOD=EKG`.
  The device catalogue already carried it, so a wearable reading and a written
  one now group into one series instead of two. `hrvRMSSD` keeps no code.
- **`总睡眠时间` resolves to `93832-4`.** Its curated row pointed at a phrase
  that resolved nowhere, so the term abstained while `睡眠时长` answered. On
  the 7,354-case evaluation: two more correct, none lost, wrong-rate unchanged
  at 0.030.
- **A reading without a unit is not coded when the unit picks the code.**
  `空腹血糖 6.1` was filed under the mg/dL code with no canonical value, and
  the series statistics left it out. Where mass and moles codes both exist it
  now waits for input (`unit:missing`); 心率 88 and 体温 38.2 still code.
- **`POST /api/data` holds a reading to the ingestion ranges too**, and its
  answer lists what it did not write, by reason (`rejected`).
- **The upload gate takes what the parsers read, and nothing else.** `.xls`,
  `.zip` and `.rar` were accepted and then failed (openpyxl opens `.xlsx` only,
  and no code opens an archive); `.tif`, `.xlsm`, `.log`, `.htm` and `.html`
  were readable and refused.
- **The agent names the file each number came off**, from the `file` column
  its query already returns.
- **The two entry crosswalk tables read in English.** `loinc_device_base.tsv`
  and `unmappable.tsv` are where a reader starts, and 136 of their rows
  carried Chinese notes. The per-vendor tables beside them are unchanged.

### Fixed

- **Deleting an account deletes it, and its sessions end.** `/user/del` had
  never deleted anything (neither statement was awaited), and a deleted
  account's JWT kept working for its 30 days: every token check (routers,
  middleware, the chat service, MCP bearer and personal URL) now asks whether
  the account still exists, and a care-circle grant from or to a deleted
  account grants nothing. Deletion needs `confirm` set to the account's email. A deleted
  address can register again: the email column's own UNIQUE, which outranked
  the partial index meant to allow it, is dropped on the next schema replay.
- **A proxy upload needs a write grant.** `POST /files/upload` did not declare
  `target_user_id`, so it was dropped and the upload filed as the caller's own.
- **Unlinking Garmin no longer answers 500 when Garmin refuses.** The link is
  removed either way; the answer says whether Garmin confirmed. Unlinking a
  provider this deployment never configured is a 400.
- **`/user/del` and `/user/update_name` without a session answer 401**, not 500.

- **`server/discover` answers in the fields the MCP SDK reads.** It sent
  `supportedProtocolVersions` and `serverInfo`; the SDK's `DiscoverResult`
  requires `supportedVersions`, so its own client rejected every discover.
- **An MCP client on 2025-11-25 can `ping`.** `resultType` went on every result,
  and the TypeScript SDK 1.30.1 rejects it on an empty one; it is sent only to
  2026-07-28 clients now, over HTTP and stdio. `initialize` over HTTP states one
  version, not a second in `_meta`.
- **`pip install 'mirobody[parse]'` and one key work outside a checkout.** The
  default config was read from the working directory only, so `mirobody parse`
  and `standardize_report` found no model anywhere else; the wheel carries it.
- **Resolver fixes from a deployment test.** 尿渗透压 and 血清渗透压 reached
  48149-9, a urine-to-serum ratio, and `serum osmolality` the calculated
  value; they answer 2695-5 and 2692-2. `Bone percentage` reached a bone
  alkaline phosphatase ratio and answers 101686-4. RMSSD spellings, which the
  trailing-abbreviation strip read as SDNN's `HRV`, refuse. `mOsm/kgH2O` and
  `osmol` normalize. The 7,354-case evaluation is unchanged (0.9158 / 218).
- **`convert_unit` converts a moles code**, and blood urea nitrogen (6299-2) is
  converted as nitrogen: it carried the whole urea molecule's mass, 2.14x off.

- **A complete catalogue no longer says it was cut.** Each catalogue row
  carries the catalogue's size, and the truncation check read it as that
  series' row count, so any catalogue of two or more answered `truncated`
  and told the model to narrow its window.

- **A symptom no longer codes as a lab analyte.** `collect.observations` sent
  every prepared row to the lexical LOINC resolver, which cannot abstain from
  a name it half recognises: a `kind=symptom` draft of 发烧 reached 153
  candidates and coded to 103717-5, Crimean-Congo hemorrhagic fever virus RNA
  in Blood. `th_concept` also takes a row for any code whose vocabulary gave
  it a name, not only the LOINC ones, so a symptom series has a standard name
  beside the words the person wrote.

## 1.5.0

② Translate is rebuilt, vocabulary and data layer together. The bundle is cut
fresh from LOINC 2.83 by one rule in one pass; every reading, whatever brought
it in, is one row of an append-only observation table with its coding beside
it, written by one module and read through one view.

Measured on 7,354 real report spellings. Over all of them coverage goes 0.963
to 0.916 and wrong-rate 0.032 to 0.030: coverage falls by construction, since
the resolver cannot answer a term whose code the cut does not carry. On the
6,992 whose expected code is inside the cut, coverage is 0.950 and wrong-rate
0.025. The other 362 expect a narrative, document or exam-finding code, or one
2.83 retired; the resolver abstains on 273 of them.

Alongside it, four things this repository said about itself that its code did
not do. Each was load-bearing: two clients were believed to depend on fields
nothing sends or reads, a picture promised a feature no endpoint implements,
and the page telling you how to start was the one page a clone did not have.

### Breaking

- **The bundle is LOINC 2.83, and half the size.** In the wheel the resolver
  data goes from 24.9 MB to 12.7, and 95.7 MB to 46.0 once unpacked; a
  `pip install mirobody` measures 68 MB on macOS, numpy included.
  `mirobody.BUNDLE_VERSION` reads `loinc-2.83+<date>-<digest>`. 63,416 of the
  99,737 ACTIVE codes, chosen by `translate_build/loinc_cut.py` rather than by
  hand: laboratory and clinical CLASSTYPE, CLASS families that never hold a
  reading dropped, narrative and document scales dropped, panels kept for the
  laboratory subclasses plus the vital-sign and personal-record ones. Rows are
  dropped, never edited; the 153 carrying a third party's copyright notice are
  dropped rather than reproduced. The members a `pip install` never opened
  (`loinc_axis.csv`, `loinc_alias_index.npz`, `fhir_dose_index.npz`,
  `loinc_demote.txt`) are gone, and `NOTICE` and `loinc_units.tsv` now ship.
- **A series key carries TIME.** The axis table gained `TIME_ASPCT`, which the
  1.4.x bundle did not have, so `series_id` is
  `loinc:COMPONENT|SYSTEM|TIME|SCALE|dim(PROPERTY)` with the third field
  filled. A spot urine protein and a 24-hour collection stop sharing one line
  on a chart, as do a heart rate and an hourly mean of one; 1,979 codes in the
  cut are `24H`. Keys written before this differ by that field — `mirobody
  recode` rewrites them and records the reason.
- **The Japanese aliases really are gone from the index.** 1.5.0's note said
  the UMLS-derived surfaces would leave with the LOINC-only re-cut; they have.
  `alias_keys.bin` is built from `Loinc.csv` and the 21 LinguisticVariants
  files alone and contains no kana. Japanese report spellings still resolve,
  through `res/resolver_overrides.tsv` — this project's own file, mapping a
  Japanese surface to an English name LOINC's index answers.
- **`mirobody/indicator/` is deleted** — 43 modules, 24,858 lines, and with it
  the `[indicator-build]` extra, `scripts/vocabulary_build.py`,
  `scripts/build_loinc_embeddings.py`, `scripts/build_runtime_index.py` and
  `docs/vocabulary-build.md`. The 2.83 bundle is cut from one LOINC release in
  one pass by `translate_build/` (~700 lines, outside `mirobody/`), so the
  passes that needed a UMLS licence, a concept graph across SNOMED CT and
  RxNorm, and a multi-GB embedding matrix have nothing left to build.
- **`resolve_with_semantic_fallback` is gone from the public API**, with the
  opt-in semantic tier behind it. It could not abstain — for an unseen term it
  returned its nearest neighbour with the confidence of a correct answer, and
  nonsense scored 0.78 where genuine names went to 0.56 — and it never ran
  anyway: the matrix was never published, so `get_index()` returned `None` in a
  wheel install and in a source tree alike, measured in both. `resolve()` and
  `resolve_reading()` are unchanged. For better recall, curate a row in
  `res/resolver_overrides.tsv`.
- **The bare-install import gate now covers the library layer**, not just the
  nine modules `indicator/` used to ship: 43 modules, each imported in a fresh
  interpreter with the extras blocked. It found nothing, which is the point —
  it was scoped to a package that no longer exists, and an empty parameter set
  reads exactly like a passing test.

- **`res/loinc_class_gated.tsv` is deleted**, with `scripts/gen_class_gate.py`
  and `scripts/build_runtime_index.py`. All 10,045 codes it gated are outside
  the 2.83 cut, so the file gated nothing; the cut does that work at build
  time, where the plan always said it belonged.

- **Apple `HeartRateVariabilitySDNN` is stored as `hrvSDNN`** (LOINC
  112429-6), no longer as the generic `hrvDatas`: five other vendors publish
  RMSSD under the same word, a different statistic, and one row would have
  averaged the two. Existing `hrvDatas` rows are untouched; new Apple imports
  land in `hrvSDNN`, and the derived `dailyAvgHrvDatas` no longer receives
  Apple data.
- **`TERMINOLOGY_VERSION` is 1.5.0.** Only a confident code is a metric's
  identity: `Metric.canonical` returns the device namespace for an unverified
  one, and `res/metrics.tsv` gains a `confidence` column.
- **`th_series_data` is retired.** `schema/30_observations.sql` creates
  `th_extraction`, `th_observation`, `th_coding_current`, `th_coding_history`,
  `th_coding_decision`, `th_coding_alias`, `th_concept`, `th_series`,
  `th_day_authority`, `th_check_result` and the view `v_observation`;
  `schema/90_retire.sql` renames the old table and its satellites
  (`th_series_dim`, `fhir_indicators`, `standard_indicators_device`) to
  `*_retired_15` and never drops them. `mirobody migrate-observations` moves
  the old rows through the new writer. `collect/readings.py`, the boot backfill,
  the indicator sync task, the `fhir_id` cache and the device registry task are
  gone with the tables they served.
- **The extraction contract is verbatim.** The file parser asks for the
  indicator name exactly as printed (never translated), the result without its
  unit, and the unit and reference range in their own fields; a printed pair
  (`120/80`) is two indicators. The model's output is frozen in
  `th_extraction` so a coding can be replayed against what was read.
- **`query_health_indicators` answers per series.** The catalogue lists series
  (`translate.series`: one axis for every cholesterol reading, in mg/dL or
  mmol/L); `indicators` accepts a series id, a LOINC code, a display name or a
  printed name; a statistic over a mixed-unit series is computed over the
  canonical values and says `mixed_units`; `change` is withheld across units.
  The semantic recall tier (an embedding of the person's own names) is
  replaced by a lexical rank over their series plus the offline resolver.
- **Corrections are rows.** The web UI's "fix this value" and "remove this
  reading" write an amendment or a retraction pointing at the old row; a
  file's date change re-files its readings as amendments; a repair batch
  retracts what it did not re-confirm. The only DELETE is the privacy path.
- **`POST /api/chat` no longer accepts `agent`, `enable_mcp`, `group_id` or
  `reference_task_id`.** They were accepted and ignored, kept for clients that
  might still send them. None does: the shipped bundle's payload is seven
  fields and none of these is among them, and no code path read one. The body
  already rejects unknown fields, so a sender now learns the field is gone
  instead of watching the value be dropped in silence.
- **One response envelope.** `utils/http.py:json_response_with_code` answered
  `{success, code, msg, data}` and omitted `data` entirely when there was none;
  `server/envelope.py` answered `{code, msg, data}` with `data` always an
  object. A client reading `data.x` had to know which router replied. `success`
  is gone, because `code == 0` says it, and `data` is always present. The
  reason recorded for keeping them apart — that the web client reads `success`
  — was wrong: its one response handler reads `code === 0 || success`, and both
  shapes always carried `code`. `apple_router.py` keeps its own shape; that one
  is the mobile client's wire.

### Added

- **The device crosswalk is a public asset.** `res/crosswalks/` gains one
  table per vendor (Apple HealthKit, Google Health Connect, Huawei, Honor,
  Samsung, Fitbit, WHOOP, Oura, Garmin, vivo, Xiaomi, Zepp, OPPO), a base
  table of 66 LOINC codes with every vendor field that means each, and
  `unmappable.tsv`, the 71 device quantities no code fits, grouped by the
  nine reasons. Every row carries a confidence and every file names the
  vendor document it was read from. `mirobody.translate.devices` loads them,
  `scripts/device_crosswalk_report.py` renders them, and
  [`docs/device-crosswalk.md`](docs/device-crosswalk.md) explains the
  normalisation traps between vendors and LOINC's own axis defects.
- `res/metrics.tsv`: 37 more device metrics carry a code (steps, the sleep
  stages, distance, floors, elevation, calories, activity intensity, body
  fat, lean mass, bone mass, daily heart-rate statistics, awakenings, sleep
  latency, and more), and ten members are new: `hrvSDNN`,
  `apneaHypopneaIndex`, `obstructiveApneaIndex`, `pulseWaveVelocity`,
  `perfusionIndex`, `walkingDoubleSupportPercentage`,
  `walkingAsymmetryPercentage`, `stairAscentSpeed`, `stairDescentSpeed`,
  `sixMinuteWalkDistance`. The Apple decoder emits the gait, perfusion and
  six-minute-walk identifiers it used to quarantine.
- A catalogue alias lets the printed unit pick the variant: glucose from a
  device that prints mmol/L codes to 15074-8, not the catalogue's mg/dL
  code. A unit that fits no sibling leaves the alias's code alone.
- Five gate invariants over the catalogue and the crosswalk: every code is
  in the axis table with a confidence, no two metrics share a code unless
  registered as one quantity at two grains, every crosswalk row names a real
  code and a real catalogue row, the Apple table says what the Apple decoder
  does, and the alias unit gate.
- `mirobody.translate`: the pure seam a reading passes through. `name_key`
  (one fold), `parse_value` (quantity / ordinal / nominal / narrative /
  absent, a number never invented), `local_day` (one implementation of "which
  day", with the zone's provenance recorded), `series_id`
  (`COMPONENT|SYSTEM|TIME|SCALE|dim(PROPERTY)`, METHOD rolled up, mass and
  molar folded), and `code()`: a confirmed alias, then the lexical resolver,
  then the scale gate; `needs-input` or `refused` with a reason otherwise.
- `collect/observations.py`: `ingest` (one transaction: freeze, fold, parse,
  place, insert, code, refresh the catalogue), `amend`, `retract`, `redate`,
  `erase`, `rebuild_series`, and the legacy-row adapter the collect layer
  still writes through. `utils.db.transaction()` runs several statements in
  one commit with a savepoint per row.
- Election writes `th_day_authority` (one row per person, series and local
  day) and records a rejected candidate in `th_check_result`.
- `observations.recode` replays the coding of every stored observation under
  the installed vocabulary, the current rules and the confirmed aliases, and
  appends a `th_coding_history` row per change with its cause
  (`recode-release`, `recode-rules`, `recode-alias`); `mirobody recode` runs
  it. `observations.confirm_alias` records what a person said a printed name
  means (or that it is not a standard item) and recodes the rows that carry
  it, so a local series merges into its standard one.
- The resolver reports what corroborated a code (`Resolution.evidence`,
  `unit_recognized`, `axes`) and refuses a code the printed unit contradicts
  (`rejected_code`); `translate.code()` stores that as `needs-input` with
  reason `unit:conflict`. Eleven printed unit spellings and the power-of-ten
  count units (`10⁴/μL`) normalize; a LOINC CLASS gate keeps radiology,
  dental and flow-cytometry codes out of lab-report answers.
- Measured on a deployment's own rows (646 file readings migrated out of
  `th_series_data`): a measure word in the name (`monocyte count`,
  `中性粒细胞计数`, `serum cystatin C`) no longer hides the analyte; a
  differential count printed as `%` finds its `/leukocytes` code; `U/mL`
  admits a tumour marker's arbitrary units, `fL` the mean cell volumes,
  `mL/min` an eGFR, `%` the distribution width; the eGFR spelled
  `mL/(min×1.73 m^2)` parses; a unit printed inside the value cell is kept
  as the printed unit, and one the tables cannot read stays unread rather
  than becoming its first readable prefix; `FT3` and `free T3` code to free
  T3 instead of T3 resin uptake; a name holding two analytes
  (`Plateletcrit (PCT)`) is `needs-input` with both codes named, not a
  refusal. `mirobody migrate-observations` reports written, coded, skipped,
  rejected and undecrypted counts.
- `translate_build/`: the LOINC 2.83 Tier-2 cut (63,391 codes) and its
  accessory tables, build-time only.
- **`docs/quickstart.md`**, the getting-started page that ships with the code.
  The README pointed at the hosted docs for this, and eleven of its links leave
  the repository. It states the three ways in — the library, the Docker stack,
  a checkout — and what each needs; every command in it was run. Two facts it
  writes down for the first time: `mirobody dev` serves the API and not the web
  client, because the built client sits outside the package, and `mirobody
  serve` finds no configuration from a `pip install`, because `config.yaml` is
  not in the wheel.

### Changed

- **`res/aliases_src/ja.tsv` is removed.** It was a UMLS-derived file
  (MSHJPN / MDRJPN) listed as a LOINC linguistic variant, and LOINC has
  none for Japanese. Two percent of its rows produced a code; on the
  7,354-case benchmark the loose file answered 51 cases correctly (organism
  and drug names) and 35 wrongly, so coverage moves 0.962 to 0.950 and the
  wrong rate 0.030 to 0.029. The Japanese laboratory names in the gate all
  still resolve. The alias index inside the bundle still carries the
  surfaces that file contributed until the LOINC-only re-cut.
  `scripts/check_wheel_data.py` forbids the file in a wheel.
- The everyday spellings of the sleep stages, skin temperature, sleep
  latency and climb resolve in English and Chinese (`深睡`, `REM sleep`,
  `皮肤温度`, `入睡潜伏期`, `floors climbed`): LOINC 2.75 names them and no
  alias table reached them.
- **`mirobody/schema/` is one file per domain.** Twenty-two numbered
  increments (`00_init_schema.sql` … `a7_…`) became nine files: prolog,
  accounts, files, observations, medications, devices, device rules, chat, and
  `90_retire.sql` for every rename and drop. Each CREATE carries its full
  column list with the `ADD COLUMN IF NOT EXISTS` upgrades beneath it. The
  catalogue a fresh database gets is unchanged, replay is idempotent, and an
  old database receiving the new chain ends in the same state (checked by
  fingerprint). One empty event-trigger function nothing used is dropped.
- **The Chinese README has an ending.** It stopped after the acknowledgements
  on an empty `<div>`: no star chart, no closing note, no copyright line. It
  ends the way the English edition does.
- **The care circle is drawn, not recorded.** `care-circle-demo.gif` (663 KB
  across both editions) is replaced by a diagram of `resolve_subject`,
  generated by `scripts/make_diagrams.py` in the same four variants as the
  C·T·A strip: two languages, two themes, 20 KB in total. Every label names
  something `user/care_circle.py` has — both `status` values accepted,
  `health_access` on the subject's own row defaulting to 0, access trimmed to
  what was asked, the raise that becomes a 403 — so the picture can be held
  against the module. The diagram deleted in 1.4.4 could not be: it promised
  "unshare a thread anytime", which no endpoint implements.

### Fixed

- **The demo seed's readings arrive coded.** The seed wrote
  `source_table="demo_seed"`, which `legacy_provenance` read back as manual, so
  its weigh-ins, steps, resting heart rate and blood pressure never reached the
  device catalogue: 6 of 2,019 readings carried a LOINC code. Upload a report
  and the same person's blood pressure became two series, `8480-6` from the
  file and `systolicPressures` from the seed, which do not merge. The seed's
  own rows are a wearable's and say so now, and its sleep rows write
  `dailyTotalSleepTime` (93832-4) rather than the uncoded `sleepDuration`
  nothing else produces. All 2,019 carry a code.
- **`deploy.sh` said "nothing is running" while three containers were up.**
  Compose starts pg and redis, then fails on mirobody, and the real cause (an
  unreachable package index, a rejected volume) is only in that container's
  log. The message now prints `compose ps` and the last 40 lines of that log.
- **A second checkout and a rootless Docker are both walls, now both
  documented.** The stack pins its subnet, so a second checkout needs a
  different one; a Docker that refuses named volumes needs bind mounts, and
  `compose.override.yaml.example` is the file to copy. Both READMEs say so at
  the deploy step, where the message finds you.
- **`vitamin B9` answered a dietary intake code.** 81066-3 is *Vitamin B9
  (Folate) intake 24 hour Estimated*, a rate on `^Patient`; a serum folate is
  2284-8, which `folate` and `叶酸` already gave. Every other vitamin and
  mineral was checked and lands on a serum code. `维生素B9` and `叶酸片` now
  resolve too.
- **Seven curated Chinese rows pointed at a phrase the index does not key.**
  `平均红细胞血红蛋白浓度` (MCHC) and `平均红细胞血红蛋白量` (MCH), both
  Simplified and Traditional, and three spellings of beta-2 glycoprotein 1.
  MCHC and MCH are on every blood count. Coverage over the 7,354 cases goes
  0.915 to 0.916 and frontier 0.496 to 0.501, with the wrong-rate unchanged.
- **`res/analyte_digit_src/` is deleted.** It was the manual overlay for
  `mirobody indicator analyte-digit`, a build command that went with
  `indicator/`; nothing in `translate_build` or the resolver reads digits, and
  the wheel already excluded it. Auditing it is what found the B9 code.
- **`LICENSE-3RD-PARTY` stopped claiming vocabularies this tree does not
  carry.** The table still listed `fhir_concept_graph.bin`, `fhir_meta.csv.gz`,
  `fhir_snomed_ct_bundle.tar.gz` and `fhir_id_map.npy`, all of which the cut
  removed and two of which the wheel gate now refuses, so the file told a
  reader they need a SNOMED CT Affiliate License and a UMLS licence to use
  this package. They are gone, the sections that cover them are marked as
  applying to 1.4.x and earlier, and `fhir_snomed_ct_bundle.NOTICE` is deleted:
  it had outlived its bundle by a release. The table now lists what ships.
- **Nineteen coded metrics had no ingestion range.** `41_device_rules.sql`
  gates a device reading's value, and the members the device crosswalk added
  (HRV SDNN's siblings, the apnea indices, pulse wave velocity, the gait
  metrics) plus every sleep total were written unchecked. Eighteen ranges
  added, `trainingLoad` exempt because a vendor score has no published scale.
  The rules are compared against the raw number, so each is written in the
  catalogue's `standard_unit`.
- **`POST /api/standardize` dates a reading by the report.** `measured_at` was
  hardcoded `None` and the write fell back to the wall clock, so a May report
  and an August one both landed on the upload minute and "latest" was
  whichever was uploaded last. Extraction reads the printed collection date
  now (`Reading.collected`), and a report that prints none still falls back to
  now.
- Three device codes were wrong and are replaced: `oxygenSaturations`
  2708-6 (a laboratory arterial blood-gas code) is 59408-5 (pulse oximetry);
  `skinTemperature` 8310-5 (core body temperature) is 61008-9 (body surface
  temperature, unverified for the wrist); `vo2Maxs` 60842-2 (oxygen
  consumption, no maximum) is 94122-9 (peak VO2 per body weight, unverified
  because wearables estimate it).
- A hyphenated component suffix is no longer stripped as an abbreviation:
  `Creatine Kinase-MB` resolves to CK-MB, not total CK; `Lactate
  Dehydrogenase-LDH1` and `Alkaline Phosphatase-BALP` likewise reach their
  own component. `Vitamin D-3` and `Complement C-3` join to the spelling the
  index knows instead of falling through to the bare stem. A printed unit may
  pick between properties of one analyte in one specimen, never move it to
  another specimen: `albumin 30 mg/24h` is refused rather than filed as
  24-hour urine albumin. On the 7,354-case benchmark three wrong answers
  became refusals and nothing else moved.
- `indicator/search.py` and `indicator/fhir/adapter.py` import on a base
  install: their `mirobody.utils` imports (aiohttp) are lazy. The adapter's
  graph expansion returns its input unexpanded when the local embedding
  matrix is absent, instead of raising on every call.
- **`mirobody doctor` on a default install says what to install.** It answered
  with a `ModuleNotFoundError` out of `utils/config/config.py`, and `cli.py`'s
  own docstring said it needed "no database and no extra". It reads the
  configuration layer, which arrives with `[app]` or `[parse]`, and it now
  checks that up front the way `serve`, `dev`, `worker` and `parse` do. A test
  fails when a command imports one of those stacks without the check.

## 1.4.4

Two stages were carrying each other's work, and both put it down. ① Collect
only collects: the indicator catalogue, the daily rollups and a second identity
implementation went to the stages that own them. ③ Agent's chat layer speaks
LangChain's names instead of a dialect invented here, and its transport is one
module rather than a class hierarchy with one implementation. The web client is
rewritten on the new vocabulary.

Nothing about the engine moved: the resolver evaluation is unchanged at 7,354
cases, coverage 0.963, wrong-rate 0.032.

### Breaking

- **The demo seeds one record and hands you four files.** The 207 KB vendored
  care-circle fixture and `demo/lab_report_2025-10-15.pdf` are deleted. The two
  sign-in accounts are renamed and are the circle now: `mom@mirobody.ai` shares
  a record with `you@mirobody.ai`, view-only, and each gets a generated year of
  device readings plus one lab panel (2,019 rows in total). Everything else
  lives in `demo/upload/` and is deliberately NOT seeded, so uploading it walks
  ① Collect and ② Translate instead of being a no-op: a PDF, a phone photo, a
  spreadsheet and a CSV, every analyte resolving to a LOINC code, and one file
  written in a second lab's vocabulary so the same code covers two spellings.
  `demo/README.md` says what each file is; `demo/generate.py` rebuilds them.
- **The Apple push endpoint speaks HealthKit.** `POST /apple/health` took a
  `FlutterHealthTypeEnum` (`HEART_RATE`) and mapped it onto the catalogue with
  its own 74-row table, while `mirobody import apple` read the same catalogue
  off HealthKit identifiers. The Flutter table described a client that no
  longer exists and is deleted: send Apple's own identifier, `startDate`/
  `endDate`, a plain `value` and `unit`. See `docs/apple-health.md`.
- **Packages moved to the stage that owns them.**
  `collect.standardize` → `mirobody.translate`; `collect.aggregate` splits
  between `translate.aggregate` and `translate.derive`;
  `collect.file_parser` → `collect.files`; `collect.apple` →
  `collect.providers.apple`; `collect.core.user` → `user.platform`; the
  scheduler and distributed lock → `mirobody.utils`. `mirobody.collect` and
  `mirobody.translate` re-export what leaves them and a contract forbids
  reaching past, so the next move costs no caller an edit.


- **A replacement agent yields `text`, not `reply`.** The block rename below
  is the plugin contract too, and the example plugin — the template a third
  party copies — was still on the old names. A plugin that stays on them is
  not an error: its answer reads as a turn that produced none.
- **The chat stream speaks LangChain's names.** `reply` / `thinking` /
  `queryTitle` / `queryArguments` / `queryDetail` / `costStatistics` / `widget`
  over one `content` field are now `text` / `reasoning` / `tool_call` /
  `tool_result` / `usage` / `interrupt`, each carrying the field
  `langchain_core.messages.content` gives it. `queryArguments` is gone: a
  `tool_call` carries its own arguments, which that block JSON-encoded a
  second time despite never being a partial delta. On a pre-1.4.4 transcript,
  where `queryTitle` carried the name alone, its contents are folded into the
  `tool_call` rather than dropped. `error` carries `message`.
  Transcripts written under the old names are renamed when they load
  (`agent.wire.blocks.upgrade`), so existing conversations still render.
- **Agent Skills are gone, and `SKILL_DIRS` with them.** The one shipped skill
  (`lab-report-walkthrough`) is part of the system prompt now. deepagents 0.7
  removed its own built-in prompt to stop competing with the caller's; a
  progressive-disclosure layer over a single document was the same competition
  one level up.
- **`agent.models.usage.cost_statistics_message` is `usage_block`**, and it
  answers a flat block of integers rather than a `costStatistics` chunk of
  strings under `content`. A consumer importing the old name gets an
  `ImportError`, which is the signal; `UsageAccumulator` is unchanged.
- **`ChatStreamRequest` is a pydantic model**, so `POST /api/chat` rejects an
  unknown field with the accepted names rather than through an `inspect`
  signature read. `ChatFileObject` is deleted (nothing constructed one) and
  `msg_id` is gone, being an alias of `question_id`.

### Added

- **A `notice` block**: the system talking to the user ("that model is not
  configured, using the default") rode the reasoning channel, where nothing
  told it apart from the model's own trace. `kernel.events.Notice` has named
  that as a defect since it was written.

### Changed

- **Four README claims were false, and are gone.** An adversarial pass over the
  rewrite checked every sentence against the code. "Four languages, one code"
  captioned a demo that shows three languages and three codes (only the
  hemoglobin pair lands on one, 718-7) — wrong since 1.4.0, and baked into the
  GIF's generator, so all four locales were regenerated. "→ FHIR R4" in the new
  diagram claimed an output nothing produces: every `resourceType` in the tree
  is mirobody READING FHIR-shaped input. "Stored verbatim" described ① Collect,
  which converts units on the way in and keeps the original when conversion
  fails; `translate/__init__.py` said the same thing and now says what the code
  does. "Cites the file behind every number" is true only of file-sourced
  readings; a device reading has no file. Also corrected: ② Translate links all
  three packages that implement it, because `translate/` is named for the stage
  but does no coding; backbone mode has no server tools, not "our data layer";
  uploaded files live in a volume, not in Postgres; a scanned page sends the
  image, not text; 326 UCUM units, not ~310.
- **The README is rewritten around C·T·A.** It said its five core claims two
  to six times each ("offline" seven times) and buried the three stages under a
  deployment section four times their length. 2,224 words to 1,605, with a new
  section that answers the question a health-data reader actually arrives with:
  what runs on your machine and what does not. The two 960×452 diagrams are
  replaced by one wide C·T·A strip that names the stages, ships light and dark,
  and is drawn for both
  editions in its own language (four files: two languages by two themes, all from
  `scripts/make_diagrams.py --check`); the care-circle diagram's four promises
  are a table in `docs/walkthrough.md`, which a diff can check and a picture
  cannot.
- **The README GIFs are re-recorded**, against the rewritten web client and the
  redesigned demo, in English and 中文: the two records side by side, a PDF
  going in and its analytes coming out coded, and the same question answered
  from two different people's files. `docs/walkthrough.md` walks all four.
- **The seams an application binds are written down** (`agent/README.md`
  §Seams), and the replacement-agent contract is spelled out on
  `registry.AbstractAgent` rather than pointing at a private function.
  `chat.turn.run` yields blocks and `chat.turn.stream` is the SSE framing over
  it, so a transport that is not SSE is five lines rather than a subclass.
- **`chat/adapters/` is one module, `chat/turn.py`.** The abstract base class
  had one abstract method, five lines of SSE framing, under 700 lines of
  pipeline a second transport would have inherited unchanged. There is one
  transport; the layer that does have a second reader is `kernel.events`.
- **The system prompt is 113 lines, from 323 plus a 68-line skill** — 19.3 KB
  to 9.9 KB, with every rule kept: `vis-chart` was specified twice and the
  response requirements three times.
- **deepagents 0.7.14** (from 0.7.5), which raises the langchain floor to 1.4.0
  and langchain-quickjs to 0.3.7. The API this repository binds is unchanged
  across the range.

### Fixed

- **The agent cited a storage key as a source.** A reading handed the model
  only `file_key`, so an answer about cholesterol named
  `web_uploads/17eaf4f6-29b0-48b9-8e88-edbee3267ea6.pdf` as where a number came
  from. `query_health_indicators` returns the document's NAME as `file` and
  shows the model that instead; `file_key` still rides in the envelope, which
  is what the web client opens the document with.
- **An extracted reading's value held its unit.** A panel printing `4.9 mmol/L`
  was stored with that whole string in `th_series_data.value`, so the
  Indicators table rendered `4.9 mmol/Lmmol/L` and nothing downstream could
  compare, average or chart the reading without parsing it first. The number
  and the unit come apart at the one write point for extracted readings
  (`collect.files.services.indicator_store.split_unit`), which is what the
  device path has always done. A value that is not a measurement keeps its
  whole self: `Negative` stays `Negative` and `120/80 mmHg` keeps `120/80`.
- **`examples/03_parse_a_lab_report.py` never parsed a file.** `parse_file` is
  async; the script called it without awaiting and died on `len()` of a
  coroutine for every path anyone gave it. Only the no-argument half worked.
- **A WebSocket upload answered in English whatever the client asked for.**
  `JwtMiddleware` is a `BaseHTTPMiddleware`, which Starlette runs for http
  scopes only, so the upload socket carried no language.
- **The agent could not read a `.docx`.** It was missing from the agent
  filesystem's copy of the extractable-extensions table, so a Word file came
  back as its zip container decoded as prose. `mirobody/documents/` is the one
  place that answers that question now, and the one place that opens a file.


- **A reopened conversation can say the turn was cut off.** The stored `end`
  block carries `finish_reason` now. The live client was told and the saved
  transcript was not, so a turn the budget ended reopened as an ordinary
  truncated answer.
- **A chat upload's `created_source_id` now names a message that exists.** With
  no `question_id` in the request the turn minted a `q_…` for the file rows and
  let `save_message` mint its own `app_…` for the message.
- **The chat layer stopped downloading every attachment for nobody.** The bytes
  were fetched, base64-ed, cached in Redis and handed to the agent, which
  documented that it ignores them: uploads reach the model through `/uploads/`.

## 1.4.3

A reading extracted from an uploaded report now carries a LOINC code, which is
the headline promise and did not hold for the main way readings arrive. Two
packages are renamed after the stage they implement, and Apple Health exports
import without the mobile app. The resolver evaluation is unmoved: 7,354 cases,
coverage 0.963, wrong-rate 0.032.

### Breaking

- **`mirobody.pulse` is `mirobody.collect`.** `pulse` was the product name this
  code was extracted from; the stage it implements is ① Collect, and 1.5.0 adds
  `translate/` beside it. 352 module paths moved. No compatibility alias: the
  import fails loudly with `ModuleNotFoundError`, which is the signal. The HTTP
  prefix `/api/v1/pulse` and the `db_config="pulse"` key are contracts, not
  paths, and do not move.
- **`mirobody.kernel.vendors` is `mirobody.kernel.decoders`.** "Vendor" and
  "provider" mean the same thing in English, so the two directory names hid a
  boundary import-linter enforces. The name was already inside the module: the
  dispatch dict is `DECODERS`. The other side keeps "provider" (505 sites, and
  `mirobody.providers` is a third-party plugin entry point).
- **`render_compact` and `render_rest` left
  `agent.tools.health_indicators_service` for `agent.tools._render`.** That
  module was both the readings tool and the other two tools' utility module, so
  the three record tools were not peers; `_base.RecordTool` now holds the
  authorization and the never-raises contract. `tools/list` publishes the same
  six tools with the same parameter counts.
- **Twenty test modules left the package.** `mirobody/tests/test_engine_coverage.py`
  is the one that ships, because the README links its score. A clone runs 33
  tests.

### Added

- **`mirobody import apple export.zip` reads the archive the Health app
  makes.** The Apple provider accepted only JSON shaped like one Flutter
  plugin's output, so a self-hosted user could not import their own data
  without running our mobile app. It needs no key, no database, no server and
  no extra. The reader streams: a measured export is 109 MB and about 446,670
  records.
- **The Apple decoder table covers what HealthKit declares**: 47 data types and
  51 metrics, including the category types whose reading is a name rather than
  a number. `record_time_ms` reads epoch milliseconds as well as Apple's own
  format. A unit is read per record, not per type, because one export holds
  `mg/dL` beside `mmol/L`.
- **`COLLECT_API_PREFIX`.** `/api/v1/pulse` is the OAuth redirect URI each
  deployment registered with Garmin, Oura and Whoop, so it cannot be renamed
  here. The default stays; a deployment that has registered nothing yet can set
  `/api/v1/collect`.

### Fixed

- **An analyte off an uploaded report had no terminology identity.**
  `GET /api/v1/health-indicators` answered `system: ""` and `code: ""` for all
  130 indicators extracted from two real checkup reports, while `mirobody
  resolve` printed the codes for the same names: `fhir_indicators` has no rows
  on a fresh deployment, the two read paths disagreed, and the fallback was
  keyed on catalogue names an extractor's free text never matches. One funnel
  answers both paths now, and it resolves from the VALUE rather than the name,
  because a report prints `Abdomen | No abnormalities seen` beside
  `ALT | 38.1 U/L` and the name alone returned 11947-9 for the first.
- **The unit was written where no reader could reach it.** The file path left
  `fhir_mapping_info` NULL and put the unit only in `comment`, which is
  encrypted; every read path selects `fhir_mapping_info ->> 'unit'`. Rows
  written before this keep an empty unit until re-uploaded.
- **The model was not handed the code it then quoted.** `system` and `code`
  reached only the `catalog` result, so asked for a reading's LOINC code the
  agent answered from memory and gave 1558-6, the [Mass/volume] glucose code,
  for a value it had just printed as 5.4 mmol/L. Every method that prints a
  value carries the identity now; `compact` hoists a constant code into one
  line, so a 200-row result pays for it once.
- **`auth_type: gcp_adc` posted `project` and `location` in every request
  body.** They derive the Vertex endpoint and are not request parameters, but
  langchain-openai moved them into `model_kwargs`. The same branch embedded the
  raw location in the URL path while normalising it for the host, so
  `location: " US "` built a 404 that named nothing. (#75)
- **A first `mirobody serve` without Postgres ended in a traceback.**
  `create_schema` opened its connection unguarded, so a database stack trace
  was the first thing a new reader saw, before the config banner. Outside
  production it now says what is wrong and starts: with Postgres unreachable
  and no keys at all, `/api/health` answers 200. Production still raises.
- **Extraction failure named providers it had not tried.** It reported "every
  configured provider returned an error" when exactly one is ever selected, and
  now names that one.

### Changed

- **The third stage is Agent, not Answer.** C·T·A named the stages
  Collect · Translate · Answer while no package was named after its stage.
  Answer would have cost 73 rename sites, Agent costs none. Both README
  editions follow, and the Chinese is written rather than translated.
- **The README no longer implies the model runs on your hardware.** Nothing
  here runs one: there is no GPU, Ollama or local-inference path in the code.
  "Offline" is scoped to the resolver, and both editions say the negative where
  the claim is made.
- **Comment style is a CI gate.** No block over eight lines, no em-dash in a
  comment or docstring (runtime strings are exempt). 1,335 em-dashes and 307
  comment lines went; both thresholds are measured against thirteen installed
  libraries rather than chosen.
- **A cross-directory import is spelled in full**, with ruff TID252 selected so
  a new one cannot land: 296 statements across 126 files. `from .sibling` is
  untouched.

## 1.4.2

The genetic tool answers like its two siblings, a `(value, unit)` pair says when
two readings are comparable, and six failures that used to be silent now say so.
Four public names leave the package — a patch number rather than a minor because
1.5.0 is spoken for by the corpus re-cut. The resolver evaluation is unmoved.

### Breaking

- **`get_genetic_data` is `query_genetic_data`, and it answers like its two
  siblings.** It was the one data tool that had not been through the 1.4.0
  tool-shell work: a hand-rolled signature instead of a declared schema, no
  care-circle `member`, no envelope, and a bespoke compact payload
  (`{"q": …, "n": …, "s": …, "_legend": …}`) the model had to learn a legend
  for. It is now the same three steps as `query_health_indicators` and
  `query_medications` — authorize, run, envelope — publishing
  `genetic_service.TOOL_SCHEMA` verbatim to the MCP and chat surfaces and
  rendering through the same `render_compact`, with the hits and their
  neighbours as ONE table (`distance` and `near` say which query a neighbour
  belongs to). The `rsid` parameter is `rsids`, an array like `indicators`,
  and a call that names none is refused rather than answered.
  What an answer says out loud, on every call: an rsID that is absent was not
  typed, and a nearby variant is near by POSITION — proximity is not linkage.
  `redirect_to_upload` is gone, and with it the branch in `mcp/service.py`
  that let any tool result replace the whole reply with "open /drive and
  upload": that redirect had one producer (this tool's no-rows path), and
  `_DATA_GATED` already hides the tool from a user with no genetic rows — so
  the only caller who could still see it was one who HAD uploaded a genotype
  file and asked about rsIDs it does not carry, where "upload your data first"
  is the wrong answer.
- **`mcp.tool.global_tools` and `global_descriptions` are gone**; the
  `get_global_tools()` / `get_global_descriptions()` accessors that already sat
  beside them are the surface now, joined by `reset_global_tools()`. They were
  module dicts that only ever grew, so one discovery leaked into the next and a
  fresh process was the only isolation. `examples/04` was reaching into the
  dicts and is the reason this is a visible break rather than an internal one.
- **`POST /vital/generate-sign-in-token` is removed.** It called
  `platform_manager.get_platform("vital")`, and no `vital` platform is
  installed — the route could only ever answer 503, behind a JWT check. The
  dead `VitalHealthRecord` model goes with it; `StandardPulseRecord` keeps its
  field set, because rows in `th_series_data` were written against it.

### Added

- **`mirobody dev` runs the server in one process with no config file.** The
  `[app]` extra and a key are still required; everything else is defaulted.
- **A personal MCP URL can be revoked.** `DELETE /personal/mcp` retires one and
  the next mint issues a fresh secret. `MCP_URL_TTL_DAYS` (default 30) replaces
  a hardcoded 365-day expiry — that URL is the whole credential, carried in a
  query path, so it reaches config files and screenshots.
- **`canonicalize(value, unit, loinc_code=...)` folds a reading to base units**
  and returns the `(value, unit)` pair. Store it beside the reading as recorded
  and two readings are comparable when their pairs agree. With a LOINC code
  `MOLAR_MASS` carries it crosses `mg/dL` ↔ `mmol/L`; without one it declines
  rather than guesses, and `%` or `mm[Hg]` comes back untouched.

### Fixed

- **A `temperature` on a Claude entry was a `TypeError`, not a setting (#72).**
  `anthropic` 1.x removed `temperature`, `top_p` and `top_k` from the messages
  API and takes no `**kwargs`, so one reaching the client failed before a
  request existed. The three are now stripped for the Anthropic families
  against the installed SDK signature, with a warning — previously they were
  only stripped when the caller asked for thinking, so the same entry worked or
  failed depending on that.
- **Vertex locations `us` and `eu` built a hostname that does not exist (#73).**
  They are multi-regional and answer on `aiplatform.<loc>.rep.googleapis.com`;
  both call sites carried only the `global` and `<region>-` forms, and one of
  them got `global` wrong too. This is the value a US-data-residency deployment
  must use, and the newest Claude and Gemini releases are often published there
  only.
- **A turn that produced no answer reported itself as a success.** Two vendors
  spent an entire output budget on `thinking` and emitted no reply text,
  finishing `stop` with no error, and the client drew an empty bubble under
  "Answer Completed". Such a turn now says so, in the caller's language, and
  reports `finish_reason: "empty"`.
- **`deploy.sh` ignored `compose.override.yaml` and reported success anyway.**
  Naming the compose file with `-f` turns off Compose's override pickup, while
  the commands the script prints for the user load it — so a host whose
  override replaces rejected named volumes got a failed `up` followed by "Up.
  Open http://localhost:18060" and exit 0. The `-f` is gone and a failed `up`
  now exits non-zero.
- **Image and Excel abstract failures logged only the exception class.** The
  PDF branch learned to log the message and the stack in 1.4.1; these two were
  left behind, so a bare `TypeError` sat next to a fallback abstract that reads
  like a success.
- **An upload logged the filename once per chunk, at INFO.** A check-up report
  is named for its subject, so a 1.5 MB file wrote the patient's name seven
  times. Both statements key on the upload session id now, and the per-chunk
  one is DEBUG.

### Changed

- **The first screen asks less of a visitor**: the opening command is
  `uvx mirobody resolve …`, which installs nothing; the demo button points at
  `/demo`, which answers over sample records with no account, where it used to
  point at a page that sent the visitor to a sign-in form; and the language
  switcher sits above the badges instead of below seven of them, for the 40% of
  readers who come to `README.zh-CN.md`. The repository also gains a social
  preview card (every share rendered GitHub's default before), `CITATION.cff`
  for the ESL-Bench paper, a code of conduct, a pull-request template, and an
  issue template for "uploaded, and no indicators came out" that asks for
  `mirobody doctor` output — seven of the eight issues outsiders have filed are
  that one failure, and it has several causes that look identical from the UI.
- **`/api/v1/pulse/theta/indicators` published an empty catalogue** and
  answered HTTP 200 while doing it: the category filter was spelled with
  underscores and the data spells those labels with spaces, so all 296
  indicators were dropped. It is the catalogue a device vendor integrates
  against; six names now select 195 of them, and a test reads the filter out of
  the source so a renamed category fails instead of emptying the route.
- **`docker compose exec mirobody python …` did not work.** The image never put
  its venv on PATH, so the command deploy.sh prints after every deploy — and
  the one the new issue template asks for five times — answered
  `python: command not found`. Also: a bind-mounted site-packages is not seeded
  from the image, so on a host that cannot use named volumes the container
  looped on `No module named pip`; `ensurepip` now runs first.
- **An unauthenticated `POST`/`DELETE /personal/mcp` is 401**, not HTTP 200
  with an error body. The envelope is unchanged and the credential never
  leaked, but a client that keys on the status code read a refusal as success.
  A care-circle refusal there is 403.
- **Seven more copies of the patient's name left the logs.** 1.4.2 moved two
  statements off the file name and left the progress callback, which fires
  about seven times per file — the "seven times" the entry was about. Those and
  six neighbours key on the upload's message id now; the PHI baseline shrinks
  608 → 604.
- **Two documented commands could not work as printed**: `examples/04` said
  `pip install mirobody` and imports the `[agent]` extra, and
  `scripts/e2e_health_data.py --user 1` always failed its PHI-canary check
  because the canary rides on the member's record, not the Demo subject — a
  permanent red for anyone who put it in CI as the document suggests.
- **The README's two megabyte figures were wrong.** A `--depth 1` clone is
  99 MB against 125 MB full, not 48 against 98; the library is 52 MB on macOS
  but ~100 MB on Linux, where numpy bundles its own BLAS. Both are named now,
  with the date they were measured. The package count, 2, is unchanged.
- **`Serum Creatinine` and `Serum Total Bilirubin` answered "Model for
  end-stage liver disease score".** Both landed on 44760-7, because the MELD
  score's long name names all three of its inputs and containment matched — so
  two different analytes MERGED onto one code and their readings into one
  series. The 中文 forms of the same trap were fixed in 1.2.x; the English ones
  were never swept, and a printed English panel writes the specimen into the
  name. Coverage is 213/213 with the two cases that now pin them; the 7,354-case
  evaluation is unmoved, which is how the pair survived — neither term is in it.
- **The gate suite is one directory: `mirobody/tests/`.** Eleven `test_*.py`
  sat in the package root and ten more beside the code they guard, which read
  like project code to anyone opening `mirobody/` for the first time. Paths
  that name them move with them — `pytest mirobody/tests/test_engine_coverage.py -s`
  still prints the published score. Nothing else changes: same 333 tests in a
  clone, same 0 test files in the wheel.
- **Each README edition argues in its own voice.** The Chinese one had become a
  sentence-for-sentence translation; both now open on the problem their own
  reader has, and `docs/testing.md` describes the suite that exists rather than
  the one that did before 1.4.0.
- **Ruff enforces six more rule families** — `B`, `ISC`, `C4`, `PIE`, `PLE`,
  `RUF100` — and `.pre-commit-config.yaml` carries ruff and `phi_lint`, the two
  gates that run in under a second. `G004`, `DTZ` and `TID252` are declined in
  `pyproject.toml` with the reason, so nobody re-litigates them.
- **A credential slice left the logs.** `providers/platform/database_service.py`
  logged the first 20 characters of a stored AES-GCM ciphertext on an
  `InvalidTag`; it is `secret_fingerprint` now, the same digest handle the OAuth
  paths use. The PHI baseline shrinks 616 → 608.

## 1.4.1

One key, every surface — decided in YAML, not in Python; and the failures that
used to be silent, said out loud. No answer changes: the resolver evaluation is
untouched.

### Breaking

- **`<PREFIX>_MODEL`, `<PREFIX>_VISION_MODEL` and `<PREFIX>_EMBEDDING_MODEL` are
  no longer read.** They chose a model per KEY, which presupposed a model table
  in Python to override. A model belongs to a `MODELS` entry now, and a
  surface's choice to `UTILS_VISION_MODEL` / `UTILS_TEXT_MODEL` /
  `UTILS_EMBEDDING_MODEL`. Setting one gets a WARNING naming where its value
  goes. `<PREFIX>_BASE_URL` stays, and now reaches chat entries too.
- **`PROVIDERS` is `MODELS`, `DEFAULT_PROVIDER` is `DEFAULT_MODEL`,
  `EMBEDDING_PROVIDER` is `UTILS_EMBEDDING_MODEL`.** In this project a
  *provider* is a device (`PROVIDER_DIRS`); the model table had borrowed the
  word. Old spellings are aliased at load time with one WARNING. `EMBEDDING_MODELS`
  is gone — an embedding model is a `MODELS` entry tagged with the vector-column
  family it writes, and a family name still resolves (`UTILS_EMBEDDING_MODEL: qwen`).
- **The Gemini SDK path is gone from the EXTRACTION surfaces**, which use the
  OpenAI-compatible endpoint like every other vendor; PDFs are rendered page by
  page there, since no compatibility endpoint takes a PDF part. The embedding
  entry stays on the REST API, the only one that takes `output_dimensionality`.
  `AIConfig`, `VisionProviderConfig`, `gemini_file_extract` and the
  doubao/zhipu/moonshot tier go with it (no caller, an SDK never installed).
  Chat entries may still be `google-genai` / `anthropic` / `google_anthropic_vertex`,
  and the shipped `gemini-flash` and `claude` are — each for a reason under Fixed.
- **`resolve_embedding_provider()` answers `""` with no usable key** rather than
  `"openrouter"`; the vector-column resolvers raise the sentence that fixes it
  instead of a 401 from a gateway nobody chose.
- **The `mirobody_pgsql` pull provider is removed.** It copied readings from a
  second PostgreSQL — a migration tool for one deployment, shipped as a device.
- **Firebase login is removed.** The seven `FIREBASE_*` settings, the web-config
  keys and the `/__/` helper routes are gone. Google and Apple sign-in stay,
  verified server-side against `GOOGLE_CLIENT_ID` and the `APPLE_*` keys. A web
  client still getting its tokens through Firebase shows neither button until it
  moves to Google Identity Services / Sign in with Apple JS.
- **Audio uploads are no longer accepted.** The handler was a stub — speech-to-text
  was never wired, so every audio file stored bytes and produced an empty summary.
  `tinytag` leaves `[parse]`. Stored objects still serve.

### Fixed

- **A `DEEPSEEK_API_KEY`-only deployment uploaded reports into silence**
  ([#68](https://github.com/thetahealth/mirobody/issues/68)). Five Python tables
  decided which provider a surface used; one knew the key and four did not, and
  whether a provider could read an image was recorded nowhere.

  Every such decision is in [`config.llm.yaml`](config.llm.yaml) now: `MODELS` is
  one table of entries (`llm_type`, `api_key` NAME, `base_url`, `model`,
  `supports_image`/`supports_pdf`, `response_format`, `reasoning_effort`, `chat`,
  `embedding`, `extra_body`), the picker lists entries whose key is present with
  the first as default, and the three `UTILS_*_MODEL` keys name what each surface
  may use — a list, so any ONE of six keys runs everything. Utility entries
  (`*-utils`, `chat: false`) are multimodal with thinking off: a text-only model
  on the vision surface is exactly #68. Python holds no model name;
  `test_one_key_defaults.py` pins the table to the entries below it.

- **Six keys now, and the three that only looked like they worked.**
  `ANTHROPIC_API_KEY` joins as the sixth. Measured against each live vendor:

  * `OPENROUTER_API_KEY`, the recommended key, could not read a report —
    `reasoning: {enabled: false}` answers `400 Reasoning is mandatory for this
    endpoint`. Now `{effort: minimal, exclude: true}`.
  * `OPENAI_API_KEY` could not call a tool — `gpt-5.6-terra` needs
    `reasoning_effort: none`, and with reasoning on it also rejects `temperature`.
  * `GEMINI_API_KEY` left the picker empty while `doctor` called chat healthy:
    the router knew the vendor's key aliases and the agent's resolver did not.

- **Anthropic uses the vendor's own API, not the compatibility layer.** There
  `response_format: {"type": "json_object"}` is REFUSED (400, "Input should be
  'json_schema'" — the compatibility page says it is ignored, and it is not), and
  a schema is taken only in OpenAI strict mode, which our extraction schemas do
  not carry. The native API constrains decoding with `output_config.format`, so
  the answer IS the document. An entry's `response_format`
  (`json_schema`|`json_object`|`none`) replaces the old boolean, because "what
  this endpoint takes" turned out to be three answers.

- **Every PDF uploaded through the web client extracted nothing, on every key.**
  The WebSocket upload buffers chunks into a `bytearray` and pypdfium2 refuses
  it, so the text layer came back empty and every model call after it was handed
  an empty string — while the upload reported success. `extract_text` normalises
  at the door, the one place every document kind passes through.

- **What an entry declares is now what gets sent.** `openai-utils` declared
  `reasoning_effort: none` and `RouteSpec` had no such field, so an
  `OPENAI_API_KEY`-only deployment extracted zero indicators. `unread_entry_keys()`
  now names any entry key no code path consumes, at boot — the class, not the
  instance.

- **Anthropic extraction returned nothing on a fresh install.** `anthropic` 1.x
  removed `temperature`/`top_p`/`top_k` and the floor was uncapped, so every call
  raised `TypeError`. The backend reads what the installed SDK's signature
  accepts and drops the rest. Behind it: the blocking call refuses a `max_tokens`
  large enough for a long report (claude-haiku-4-5 — 20000 passes, 32000 raises),
  and extraction asks for 32000, so every request streams. Floors are
  `anthropic>=1.5` and `langchain-google-genai>=4.4.0`, uncapped.

- **A `GEMINI_API_KEY` agent could not finish one tool call.** Gemini 3.x attaches
  a `thought_signature` to every function call and requires it back verbatim; the
  compatibility endpoint cannot carry it, and no switch on that path recovers it.
  `gemini-flash` is `llm_type: google-genai` — the one chat entry that is not
  OpenAI-compatible. Extraction is unaffected: one call, no second turn.

- **A PDF attached to a question 400'd, on the default chat model of the
  recommended key.** `read_file` delivers it inside a `ToolMessage`, and whether
  that is accepted is a property of the TRANSPORT: OpenRouter answers `400 tool
  messages must include a non-empty string tool_call_id`, OpenAI `400 Invalid
  value: 'file'` — both for models that genuinely read PDFs over their own vendor
  API. The transport decides now, and a profile may only narrow, never widen:
  that field has three origins and two describe the model. On an
  OpenAI-compatible endpoint a PDF is served as extracted text, which works.

- **Zero indicators now says why.** Four states rendered identically: no provider
  configured; a provider that failed every call; an extraction that raised; a
  document with genuinely none. The first three now mark the file failed with the
  sentence that fixes it — the third used to be written `complete` with
  `error: ""`, under a placeholder abstract reading "uploaded successfully".
  Underneath, the vision path caught every exception and returned `""`, so a
  `400 This model does not support image` looked exactly like a blank page.
  Selection happens once; a call that then fails is a failure, not a reason to
  try the next entry. Server and worker log one WARNING per surface without a
  provider, and an ERROR when there is none. The files API exposes the row's
  `error`, which it had withheld while the fix sat in the database.

- **The indicator sync sweep died without an embedding key.** `embed()` raises on
  a misconfigured `UTILS_EMBEDDING_MODEL` — correct for a direct caller — and the
  sweep let it reach the consumer loop, so an Anthropic- or DeepSeek-only
  deployment saw a traceback per signal instead of the documented fallback. The
  step is skipped with one warning and the sweep finishes.

- **The agent told users to re-upload a report it could see.** The prompt said
  `/uploads/` holds "the file I just uploaded", but that mount is only the
  current message's attachments; a Data-page upload lands in `/library/`.

- **An entry's thinking configuration was overwritten**
  ([#70](https://github.com/thetahealth/mirobody/issues/70)).
  `_vertex_anthropic_kwargs` assigned `model_kwargs["thinking"]` unconditionally,
  so an entry declaring the adaptive shape for Claude Opus 4.7+ had it replaced
  by a budget shape those models reject. On the OpenAI path an entry carrying a
  `reasoning` dict also received `reasoning_effort`, which `Responses.create()`
  rejects before any request. What an entry declares now stands.

- **The Indicators tab listed nothing, against a healthy endpoint**
  ([#62](https://github.com/thetahealth/mirobody/issues/62)). The shipped bundle
  predated the route's reshape and read keys it never sends. Three reads were
  wrong, not the one reported — the list, the readings drawer, and the
  **per-reading edit and delete buttons**, whose key is `row_id` where the client
  looked for `id`, so owner-only correction had disappeared from every row.
  Client-side only. `test_indicator_contract.py` pins the key sets here.

- **`./deploy.sh` picked a Docker mirror by asking the wrong process.** The probe
  was a `curl` from the shell, which has `http_proxy`; the daemon that pulls does
  not inherit it, and the probe hit the website rather than the registry. It pulls
  the smallest real image through the daemon now. Separately, `ENV` and the two
  encryption keys were written only into a `.env` the script created itself —
  dropping in one that holds just an API key left every boot missing them.

- **Two gates that were missing, both key-free and on every PR.**
  `test_upload_smoke.py` puts a real PDF through the real upload path;
  `test_file_block_transport.py` pins the transport rule in both directions.
  Three defects above were invisible for a release because nothing did this.

### Changed

- **The bundled web client is half its size**, 7.0 MB in 59 files to 3.7 MB in 42.
  The Vite legacy plugin emitted a second transpiled copy of every bundle for
  browsers that could not render the app anyway: it transpiles JavaScript only,
  and the stylesheet needs `@property`, `color-mix()`, `@layer` and `oklch()` —
  Tailwind v4's floor of Chrome 111 / Safari 16.4 / Firefox 128. Those targets
  were getting a working script and an unstyled page. Nothing changes for a
  browser that runs the app today; the floor is written down in `browserslist`
  and [`docs/frontend.md`](docs/frontend.md).

- **A `git clone` is half the size.** Deleting code does not shrink a repository —
  every blob stays in the pack. What a clone pays is 29 MB of git objects plus
  69 MB of Git LFS, and the LFS half is the slow one. Three LFS files no runtime
  reads (`scripts/check_wheel_data.py` has forbidden all three since 1.3.0) are
  release assets now, listed with checksums in
  [`mirobody/res/EXTERNAL.tsv`](mirobody/res/EXTERNAL.tsv) and fetched by
  `scripts/fetch_data.sh`, which `./deploy.sh` and CI call. This works without
  rewriting history, because git-lfs only fetches the checked-out commit's
  objects: with `--depth 1` a clone goes from **~98 MB to ~48 MB**. Semantic
  indicator search degrades to the lexical index when the concept graph is
  absent, exactly as it already did on a `pip install`.

- **The configuration is split by concern.** `config.yaml` keeps what the
  containers wire; `config.llm.yaml` is the file to open; `config.devices.yaml`
  holds Garmin/Oura/Whoop and the mobile sign-in block. An `INCLUDE` list at the
  top of `config.yaml` names them, and they load before your `config.{ENV}.yaml`.
  Gone with the move: `FRONTIERX_*`, `JINA_API_KEY`, a duplicate
  `DASHSCOPE_API_KEY`, and the `*_API_KEY: ""` lines that read as "put the key
  here" when the key goes in `.env`.

- **`.env` is where the key goes, and the boot log answers whether it worked.**
  `./deploy.sh` writes the key names into `.env` and no longer lists them in the
  generated overlay — there were two places to put a key, and #68's reporter
  chose the other. The boot banner ends with the table `mirobody doctor` prints:
  what each surface selected, and the fix where one has nothing.

- Model ids verified against each vendor's live catalogue on 2026-09-10:
  `gemini-3.8-flash`, `gpt-5.6-terra`, `anthropic/claude-sonnet-5`,
  `qwen3.8-flash`, `deepseek-flash`, `claude-haiku-4-5`. Network wording names
  the condition (openrouter.ai unreachable), never a region.

- **A dead-code sweep**: 52 files, +245 / −1,978 lines. Everything with zero
  callers here, downstream and in the tests — `utils/s3.py`, `utils/truncate.py`
  and with it the `tiktoken` dependency, `utils/data.py`, the sync
  PostgreSQL/Redis paths, the storage backends' `get_file_info`, the scheduler's
  unwired stop/status methods, and a dozen pulse service methods.

## 1.4.0

**One framework, three installs.** The pure rules live in `mirobody.kernel`;
the agent harness, the document reader and the utilities are libraries a
consumer imports; the server here is one consumer of them. A reading's day is
decided once, at write time; three surfaces read through one tool per data
class.

### Breaking

- **`query_health_indicators` keeps its name, loses five parameters, and
  medications get `query_medications`.** One tool per data class, every
  parameter applicable to every call: readings
  (`keywords | indicators, start, end, resolution, aggregate, limit, member` —
  eight), medications (`view=plan|log|history, keywords, start, end, member` —
  five), genetics. A draft of this release had folded medications into the
  readings tool behind a `kind` switch: seven of thirteen parameters were then
  invalid for one kind and every misuse cost a refusal round-trip, while the
  one promised benefit — both classes in one call — was never real, because
  `kind` was single-valued. Parameter changes to the readings tool:
  `start_time`/`end_time` → `start`/`end`; `aggregate="day|week|month"` →
  `resolution=` (and `stats` honours it: at `day` it averages DAYS); new
  `aggregate="latest"` and `member`; gone: `panel` (a panel is its member
  names), `exclude` (the catalogue is the second round), `order` (a baseline
  is `aggregate=stats`: `first`/`first_date`), `source` (rows carry theirs).
  The schema is `kernel.query.TOOL_SCHEMA` published verbatim to the agent and
  to MCP; an unknown parameter is a structured refusal by name. `tools/list`
  hides each data tool from a user who has none of that data.
  `GET /api/v1/health-indicators` gains `resolution` and narrows `aggregate`
  to `none|stats|latest`.
- **A comma no longer splits a list element.** `["1,25-Dihydroxyvitamin D"]`
  is one name; a bare string the model composed (`"LDL, HDL"`) still splits.
- **Import paths.** The pure modules are `mirobody.kernel.*` (`metrics`,
  `series`, `quality`, `overlay`, `meds`, `query`, `tools`, `ops`, `connect`,
  `sink`, `events`, `evidence`, `memory`, `vendors/`); `from mirobody import
  series` is `from mirobody.kernel import series`. The vocabulary layer
  (`engine`, `units`, `lexical`) stays where it was.
- **Agent config keys lose `_DEEP`:** `PROVIDERS`, `PROMPTS`, `ALLOWED_TOOLS`,
  `DISALLOWED_TOOLS`, `DEFAULT_PROVIDER`. **The old spelling still works** —
  it is renamed onto the new one as each config file merges, and says so once
  in the log. The alias is deliberate rather than a shim: the failure without
  it was silent, because the shipped `config.yaml` declares four of these
  itself, so a 1.3.x overlay carrying only `PROVIDERS_DEEP` was shadowed by
  the default and the agent booted with zero providers and an empty
  `/api/models`, with nothing to point at. Renaming at LOAD time — not as a
  read-time fallback, which the shipped default would have pre-empted — keeps
  ordinary layering: a later file's old spelling still overrides an earlier
  file's new one, and the environment variables alias the same way.
  `PRIVATE_AGENT_DIRS` and `MCP_RESOURCE_DIRS` are gone, and so are
  `HEARTBEAT_INTERVAL` / `HEARTBEAT_COUNTER_THRESHOLD`; those four are NOT
  aliased (the heartbeat's replacement has different semantics, and the
  directory keys have no successor) but a config that still carries one is
  named in the log instead of ignored in silence.
- **`/api/models` returns bare provider names**, `/api/prompts` returns
  `{"system": [...]}`, the chat request's `agent` field is ignored. Removed:
  `/api/agents`, `/api/providers`, `/api/user/prompt*`, `/api/user/mcp*`. The
  shipped web client calls none of them; `POST /personal/mcp` and `/mcp` are
  unchanged.
- **Provider contract.** `Provider.format_data(fmt_input)` is the one method
  (`.context` + `.payload`; `format_data_v2` gone); `_validate_credentials(
  credentials)` is the one credential hook, and `POST /{platform}/token` now
  actually calls it. `format_data` no longer returns medications.
- **The chat heartbeat:** `HEARTBEAT_INTERVAL` × `HEARTBEAT_COUNTER_THRESHOLD`
  (first ping after 40 s) is replaced by `SSE_HEARTBEAT_SECONDS` (default 8,
  `0` disables), fired on silence.
- **`eval/run_eval.py` is `benchmarks/run_eval.py`.** Tests moved from the
  package to `tests/` at the repo root, mirroring it; the wheel build fails if
  a test file reappears inside the package.
- **Where things live.** `mirobody/agent/` was 24 flat modules; the ones that
  answer one question together are now packages — `agent/models/` (build a chat
  model, read its messages, count its tokens), `agent/filesystem/` (the virtual
  filesystem: the backends, naming, upload-time preparation), `agent/wire/`
  (LangGraph's stream → what a client renders). `attachments.py` folded into
  `prompt.py` (both are text the harness writes for the model), `filetype.py`
  into `filesystem/naming.py`, and `cache_config.py` is gone — four of its five constants
  had no reader. The `HealthQuery` implementation left `agent/tools/`, where
  every other file is a tool the MCP loader publishes, for `mirobody/collect/
  query.py`, beside `readings.py`, the one WRITER of the table it reads.
- **The artifact carries only what an install can run.** Five modules were
  shipping that raise on import from a wheel: `indicator/main.py` (it imports
  the two pruned build subtrees), `fhir/bridge.py` and `fhir/siblings.py`
  (polars, which only `[indicator-build]` provides), and `fhir/adapter.py` +
  `fhir/inspect.py` (they imported the pruned `embeddings/`). The last one
  mattered at runtime: `pulse/query.py` reaches `FhirAdapter` for semantic
  recall inside a broad `except`, so on a pip install the semantic tier failed
  at import and fell back to lexical without saying so. Fixed by moving the
  READER out of the build tree — `fhir/embeddings/local.py` is
  `fhir/index.py`, the same split `mirobody/_bundle.py` records for the
  tarball and stopped one module short — and by pruning the build CLI and its
  passes from the wheel and sdist. The indicator package now ships nine
  files, all of them importable, instead of twenty of which five were not.
  Also pruned: `fhir/loinc_lookups.py` (98 KB of generated LOINC Part tables
  that nothing imports) and the loose `res/analyte_digit_src/` build input.
  Dead code removed with it: `common.MappingRow`, `FhirAdapter._resolve_local`,
  and `resolve/axis.axis_rerank_bonus` with its weight table — a documented
  soft per-axis bonus that `git log -S` shows never had a caller in its
  history, while the adapter applies the hard-argmax bonus inline.
- **One parser for a bundle member that is a code list.** `loinc_skip.txt` was
  parsed in three places — the resolver's skip set, the semantic tier's, and
  the embedding index's mask builder — and the third did not drop `#` comment
  lines, so a commented entry would have reached `code_to_int` and raised.
  The member carries no comments today, which is why nothing caught it.
  `_bundle.read_code_list` is the one reader; each caller keeps its own shape.
- **The vocabulary build CLI is `scripts/vocabulary_build.py`.** It was
  `python -m mirobody.indicator`, which made the entry point of a build
  toolchain into library code — it shipped in every wheel while importing the
  subtrees the wheel prunes, so the documented command raised
  `ModuleNotFoundError` on any published install. It joins the three sibling
  passes already at the repo root, for the reason `benchmarks/run_eval.py`
  moved there earlier in this release. Each subcommand now imports its own pass
  on dispatch: `--help` and the offline `normalize` run without the build
  dependencies, and a missing one is reported against the pass that needs it
  (`siblings: needs polars`) instead of failing at start-up. Verified by
  installing the wheel into a clean virtualenv and running the README's
  documented commands against it.
- **Documentation moved to where its rule says it goes.** The second half of
  `mirobody/indicator/README.md` was a 446-line manual for rebuilding the
  terminology bundles from raw UMLS / SNOMED CT / LOINC / RxNorm releases —
  contributor documentation shipping to every install, against this
  repository's own convention that a package README is short and long-form
  guides live in `docs/`. It is [docs/vocabulary-build.md](docs/vocabulary-build.md).
  The section documenting `mirobody/units/` went to that package, which had no
  README, and the architecture block was three releases stale: it listed
  `lexical.py`, `zh_fold.py`, `value_scale.py` and `fhir/units/` inside the
  package, all of which moved to the package root in 1.3.0.
  `mirobody/indicator/requirements.txt` is gone — a second, incomplete
  declaration of the `indicator-build` extra that nothing read.
- **Authentication is `mirobody/user/auth/`.** Seven modules and about three
  thousand lines of sign-in mechanics (tokens, emailed codes, Apple, Google,
  Firebase, passkeys, the OAuth flow) sat beside the identity records under a
  package whose one-line description was "identity and the care circle".
  `user_service.py` is the seam that picks a validator and lands an account.
- **The demo fixture left the package.** `care_circle_demo.json.gz` and the lab
  report it asks you to upload are the repo-root `demo/`, beside `frontend/`
  and for the same reason: the application is `git clone && ./deploy.sh`, so
  230 KB of synthetic data was dead weight in every LIBRARY install.
  `mirobody/demo/__init__.py` is `mirobody/server/demo.py`, next to the
  bootstrap that calls it; `DEMO_DATA_DIR` overrides the location and a
  deployment with no fixture logs and starts anyway.

### Added

- **Three installs.** `[parse]` reads documents; `[agent]` is the harness as a
  library — middleware, virtual-filesystem backends, Postgres checkpointer,
  model-client factory, `hitl`, prompt renderer, the tool surface — with no
  HTTP server; `[app]` is both plus the server. `mirobody.utils` resolves its
  re-exports lazily (PEP 562), so a leaf helper imports alone.
- **`mirobody.kernel`**, with `__init__` drawing the stage → module map.
  New in it: `meds` (plans, scheduled doses by `(plan, local_date, slot)`,
  taken/skipped events, adherence, a small dose grammar, FHIR readers, and the
  `query_medications` contract with its three pure row projections; storage
  and terminology are ports), `overlay` (a correction is a layer applied at
  read time, never a rewrite), `vendors` (Garmin/WHOOP/Oura decoders as pure
  functions with shipped samples; `open_wearables` with a 93-row crosswalk),
  `connect`/`sink`/`ops`/`tools`/`query`/`events`/`evidence`/`memory`
  (contracts with the reference server as first consumer). Five
  body-composition metrics join the catalogue: 305 members.
- **`mirobody.documents`.** `detect.kind` by extension, content type and
  bytes; `extract.extract_text` by kind — PDF text layer page by page with
  ONLY scanned pages OCR'd (injected `Ocr`, concurrent), images downscaled
  then OCR'd, workbooks as markdown under a row budget, Word/PowerPoint as
  markdown, text through the encodings reports are saved in. `pipeline.py` is
  the two-phase upload (store, answer, then extract → sidecar → mine → status
  `pending|ok|empty_text|partial|failed`) with every product decision a
  `Ports` port. `engine.parse_file` extracts text first, so a born-digital
  report needs a text model key only.
- **`mirobody.agent` as a library:** `harness.assemble` (both halves of the
  no-subagent configuration, resolved for THIS model instance; read-only
  mounts; the middleware stack in order), `events_bridge` (LangGraph stream →
  `kernel.events`; every parallel tool result is reported, reasoning reaches
  `thinking`), `clients.build_chat_model` for every provider family
  (OpenAI-compatible with reasoning capture, Azure WIF, Claude on Vertex with
  cache breakpoint and thinking budget, Gemini on Vertex or AI Studio),
  `messages`, `usage.UsageAccumulator` (`costStatistics` gains
  `thought_tokens`, drops `cache_creation_tokens`), `document_backend`, `filesystem.naming`.
- **`mirobody.utils`:** `sse` (silence-triggered keepalive, `sse_headers`),
  `net` (`assert_public_url` judges every resolved address and every redirect
  hop; `fetch_bounded`), `llm_output` (sentinel and wrapper stripping, tolerant
  JSON), `prompts` (one `StrictUndefined` Jinja environment),
  `db.use_engines` (engine by injection), `log.user_tag` (keyed pseudonym).
- **`mirobody.testing`:** `phi_lint` (log statements carry ids, counts and
  type names only, with a shrink-only baseline), `prompts.lint_prompt`,
  `samples`, `contracts.check_sink`, `golden`, `coverage` (what a connector
  carries, generated from its decode table).
- **Plugins by `pip install`:** entry points `mirobody.providers`,
  `mirobody.tools`, `mirobody.agents` beside the directory keys; example in
  `examples/mirobody_example_plugin/`.
- **The day columns** (`schema/a4_series_data_day_authority.sql`):
  `local_date`, `series_key`, `source_class`, `fingerprint`, `elected` derived
  at write time through the metric's own window; `pulse/backfill.py` fills
  history at boot; an unfilled row reports `window_semantics` instead of
  pretending. **Write-side election** (`pulse/aggregate/election.py`): measurer
  > profile echo > coverage > measurement instant > deployment priority.
- **Medication storage** (`schema/a5_medications.sql`, `pulse/meds/`); Apple
  clinical records import medications. Provisional; see `docs/medications.md`.
- **Retry governance:** a call whose `(tool, normalised arguments)` already
  failed unrecoverably is refused; a per-turn cap removes the data tool rather
  than ending the turn.
- **The pull loop backs off:** three authorization failures expire a
  credential, each costs a cooldown.
- **`<PROVIDER>_MODEL`** and **`<PROVIDER>_EMBEDDING_MODEL`** per provider;
  `<PROVIDER>_BASE_URL` now reaches vision and structured extraction too
  ([#52](https://github.com/thetahealth/mirobody/issues/52)).
- **Readings know where their date came from**
  ([#53](https://github.com/thetahealth/mirobody/issues/53)): `report_date`,
  `date_source`, `date_confirmed` on file rows; `POST
  /api/v1/health-indicators/file-date` re-files one file's readings; the date
  is probed first and pushed as `report_date_detected` on the upload socket;
  in chat the agent asks with the new `ask_user` interrupt (agent-only, not on
  MCP).
- **Backup and restore documented** (`docs/backup-restore.md`, `shell/backup.sh`).
  Docs: `docs/pipeline.md`, `docs/answers.md`, `docs/medications.md`, README
  acknowledgements in four languages.

### Changed

- **Ten dependencies gone, about 470 MB less in a fresh install:**
  `langchain_community`, `pypdf`, `rarfile`, `py7zr` (declared, never
  imported); `langchain-google-vertexai` + `google-cloud-aiplatform` (171 MB;
  `init_chat_model` names the package a deployment configuring Vertex must
  install); `langchain-anthropic` (deepagents requires it); `mutagen`,
  `httpx`, `pytz`, `pdfplumber` (a second library for a job one already does);
  `pandas` (68 MB for two functions — `openpyxl` reads `.xlsx` directly).
- **One writer for `th_series_data`** (`pulse/readings.py:upsert_readings`,
  with the four collision policies named) and **the quality gate runs on every
  row**: impossible spans, future starts, non-finite numbers and percentages
  outside 0–100 are rejected, counted by reason code.
- **`kernel/query.compact`** is the one table renderer (columns inferred,
  containers summarised, 500-character cells, `empty=`).
- **The system prompt renders strictly:** a missing variable is the error the
  client sees, not a raw template sent to the model.
- **`evidence` and `memory` returned to the kernel** for the first consumer's
  insight detector and profile watermark (`mirobody:rewritten-at=`).
- **`/mirobody.json` emits every flag the web client reads** — mobile sources,
  `MCP` in new features, API config derived from what is installed; before,
  the device-provider UI was invisible on every deployment.
- **One response envelope** (`server/envelope.py`), **module loggers
  everywhere** (no emoji), and **a ruff gate** in CI with a narrow rule set.

### Fixed

- **Health data reached the logs:** the MCP handler logged the first 100
  characters of every result; the chat path logged tool arguments and results
  and streamed raw driver exceptions (which quote bound parameters). Logs now
  carry ids, counts and type names; clients get `client_safe_error`;
  `PHIPolicy` is on the root logger and `phi_baseline.txt` can only shrink.
- **`LIKE '%sleep%'` decided a reading's day:** it re-anchored 58 vendor-dated
  daily metrics by a day and missed `napDuration`. The catalogue's `window`
  decides. Note: the next aggregation writes those 58 metrics a day later than
  the last one did.
- **`vo2Maxs` declared `L/min/kg`** (unparseable, 1000× out); it is
  `mL/min/kg`. Stored values were always mL/min/kg.
- **A FHIR resource id was a cross-person medication plan id;** two people
  importing from one clinic collided. `plan_id_for(subject, record, concept)`.
  A dosage carrying only `Dosage.text` produced no schedule; it is parsed.
- **WHOOP calories ×4.184 instead of ÷; zone minutes ÷ 1/60000 instead of
  ÷ 60000.** Garmin sleep stages decoded to names no catalogue entry matched.
- **The chat model saw `Any | None` for every tool argument;** the MCP
  `inputSchema` is passed through, so `aggregate: none` fails validation. A
  tool taking `**kwargs` had every argument dropped. Three helper methods of a
  tool class were published as MCP tools (`__tools__` allow-list).
- **`eval` could not reach the data** (no `ptc`); the read-only tool is
  `tools.query_health_indicators` inside the REPL, with a per-eval budget. The
  prompt named two tools that no longer existed and a limit the runtime did
  not enforce; `tests/test_prompts.py` fails the build on either.
- **A day query padded its window a day either side;** it is an equality on
  `local_date`. Keyword recall ran the vector search first; the lexical rank
  over the person's own catalogue runs first. A window the caller did not ask
  for was reported as if the rows came from it.
- **A `tools` node running several calls in parallel reported only the last
  result.** `apple_router` sent `cdaData` as a list while `format_data`
  expected a dict; both shapes read.
- **The Volcengine tier had never run** (vendor SDK not a base dependency); it
  uses the OpenAI-compatible client like every other provider. An
  `ANTHROPIC_API_KEY`-only deployment got silence from the utility helpers;
  `claude` left the auto-selection list.
- **`GET /api/data` without a filter returned 500** (an untyped NULL bind).
- **CI's full suite had been the minimal suite** — both workflows installed a
  removed extra; it is `'.[app,test]'`.

### Removed

- **The ChatGPT Apps widgets and the MCP `resources` capability**
  (`agent/resources/`, `mcp/resource.py`, `MCP_RESOURCE_DIRS`): no tool ever
  pointed at them, and `resources/read` templated the caller's JWT into HTML.
- **Per-user prompt additions and the per-user MCP-server store**
  (`user_agent_prompt`, `user_mcp_config`): nothing in the client wrote them,
  nothing read them. The system prompt has one source, `PROMPTS`.
- **`BaseAgent` and the provider-native loops** (~1,500 lines, off by
  default); `DeepAgent` is `MirobodyAgent`, `agent/deep/` and `agent/utils/`
  are folded into `agent/`, `chat/agent.py` is `agent/registry.py` (the first
  class found is THE agent; `AGENT_DIRS` replaces, never adds),
  `chat/user_profile.py` is `user/profile.py`, `deep.jinja` is
  `mirobody.jinja`, the persona is `AGENT_NAME`.
- **`volcengine-python-sdk[ark]`** (245 MB for one call `openai` makes) and
  the dead LLM-config surface around it.
- **Dead code from the Vital era and the v1/v2 migration:** `pulse/core/
  constants.py` and `models.py` types nothing imported, the three-level
  database-service hierarchy, a 140-line magic-byte sniffer, the
  `pull_from_vendor_api_optimized` hook no provider defined.

## 1.3.0

**`pip install mirobody` is a library:** 2 packages and 52 MB instead of 93
and 245 MB. Document parsing is `mirobody[parse]`; the server is
`mirobody[app]`. `[agents]`, `[server]` and `[cn]` are removed — all three
map to `[app]`.

### Changed

- Base dependencies are `["numpy"]`. `mcp>=2.0.0` in base had made the
  package uninstallable beside `langchain-mcp-adapters` (`mcp<2`); it moved to
  `[app]`.
- `mirobody parse` checks for its extra up front and prints the pip command.
- The vocabulary layer (`units`, `lexical`, `value_scale`, `zh_fold`) moved
  from `indicator.fhir.*` to the package root.

### Added

- `mirobody.bundle` — the stable build-time surface over the LOINC axis
  table and alias sources (`load_axis`, `read_member`, `bundle_version`, …).
- `py.typed`; a declared public surface (`from mirobody import resolve,
  resolve_reading, parse_file, Resolution, Reading`, lazily);
  `mirobody.BUNDLE_VERSION` (`loinc-2.82+<date>-<digest>`) with a CI stamp
  check; `eval/run_eval.py` ships with `--testset`.

### Fixed

- **A percentage and a count are two analytes.** `中性粒细胞` at `62 %` is
  26511-6, at `4.2 10*9/L` is 26499-4; all five white-cell lines in four
  spellings, and the inline unit is read out of the value.
- `Hemoglobin A1c` answered 41995-2 while `HbA1c` answered 4548-4.
- A trailing acronym that REPEATS the stem is stripped (`总胆固醇 TC`); one
  that NARROWS it is not (`Protein CSF` abstains).
- `resolved=True, loinc=""` is unreachable: a deprecated corpus row no longer
  wins over the LOINC-bearing row behind it (+3.4 points coverage).
- A switched variant reported the pre-switch name; `呼吸频率` did not resolve;
  `eGFR` answered a heart-failure marker; a curated 乙肝e抗原 row pointed at
  nothing; `resolve()` now honours `loinc_skip.txt`.
- **The resolver holds 70% less and loads 3× faster** (514 → 156 MB, 1.09 →
  0.28 s cold, 0.76 → 0.07 ms per call): byte blobs plus int32 offsets
  instead of a pickled object array and CSV text. Identical on all 7,354 eval
  cases.
- The semantic tier's safety gates cannot silently vanish; one copy of the
  alias tables; the runtime no longer imports the build tooling; `boto3` is
  declared.

### Removed

- 17 MB of bundle members and 19,000 lines (`indicator/fhir/embeddings/`,
  `indicator/fhir/resolve/`) that no install can read or run — pruned from
  the wheel, kept in git, guarded by `scripts/check_wheel_data.py`.

## 1.2.2

### Added

- `indicator.fhir.units.pick_display_unit` — the unit a merged series
  displays in (most frequent, ties to the latest).

### Fixed

- `parse_value_unit` glued `240 10⁹/L` into `240109/L`; the author's
  whitespace boundary is respected.
- The pulse conversion tables drifted from the UCUM engine (lb, glucose molar
  mass); they derive from it now.
- An attachment-only chat turn was refused as empty; it becomes a stand-in
  question in the request's language (#39, #41).
- `/uploads/` paths moved under the model mid-turn as the descriptive
  filename arrived; the turn's attachments keep the names they were attached
  under (#40, #42).

## 1.2.1 — released 2026-08-23

The first release in which `pip install mirobody` works: 1.0.62 shipped no
CLI, no `mirobody.engine`, and LFS pointer stubs for its data. Version jumps
1.0.62 → 1.2.1. Checkout deployments: the MCP tool rename below is the one
real break; `/chat` and `/drive` redirect to `/ask` and `/data`; schema files
never drop columns.

### Breaking

- `search_health_indicators` + `fetch_health_data` → one
  `query_health_indicators` with server-side `aggregate`, a discovery mode,
  compact pipe-table results and a `limit` ceiling.
- A user's saved prompt is APPENDED to the system prompt instead of replacing
  it. `GET /api/prompts` takes `?agent=`.

### Added

- **One key runs everything:** `OPENROUTER_API_KEY` or `DASHSCOPE_API_KEY`
  drives chat, vision and embeddings; `<PROVIDER>_BASE_URL` for self-hosted
  endpoints; `PRODUCTION: true` refuses placeholders and demo codes;
  `BOOTSTRAP_SCHEMA: false` turns off DDL replay.
- Every demo account owns a thin record beside the shared synthetic one.
- A zero-key deployment marks uploads *failed* with the reason; `/api/models`
  lists only providers whose key resolves.
- MCP 2026-07-28 (stateless revision); `resolve_indicator` and
  `normalize_unit` over MCP; Agent Skills at `/skills/`; `mirobody parse` /
  `resolve` CLI; the resolver coverage benchmark in CI.
- `GET /api/v1/health-indicators` and `POST .../reading` (correct or
  soft-delete one reading); data-gated `tools/list`.
- `POST /api/standardize`, `POST`/`GET`/`DELETE /api/data` — shaped like the
  hosted API, without `/v1`, tenancy fields or an implicit delete-all;
  `engine.parse_text`.

### Fixed

- **Resolution.** NFKC-lite folding and a CJK tokenizer (`LDL–C`,
  `fasting_glucose`); `名称(缩写)` resolves and refuses when the halves
  disagree (0% → 100% on 6,641 terms); device vocabulary (`steps`, `SpO2`,
  `静息心率`, …); the abbreviation column (`HGB` had answered HbA1c, `HCT`
  calcitonin; `CA`/`PT`/`MG` are refused). Coverage 32/94 → 211/211.
- **Security.** Deleting a document did not stop the agent reading it (the
  workspace copy and the profile summary are withdrawn inline); cross-user JWT
  leak in `resources/read`; stored SQL injection in `get_genetic_data`;
  `KeyError` on a JSON-RPC request without `id`; credentials in error logs;
  care-circle sharing broken by a one-argument `isinstance()`; raw exception
  text to callers; ages one year too high.
- A malformed tool call ended the turn as an empty answer;
  `InvalidToolCallRepairMiddleware` bounces it back to the model.
- Local-storage file URLs hard-coded `localhost:18080`; the upload gate
  rejected audio, HEIC and Markdown its handlers accepted.
- **Packaging.** Wheels carry the data bundles (LFS in CI, `package-data`);
  undeclared `numpy`/`anthropic`/`python-multipart` declared; the web client
  left the wheel; `utils/config` and `utils/db` no longer import the database
  stack at module scope, so the engine runs with numpy alone; tests no longer
  ship; DDL moved to `mirobody/schema/`; `MANIFEST.in` so the sdist installs.

### Changed

- `costStatistics` reports tokens only; the dollar table is gone.
- MCP schemas come from the official SDK (`func_metadata`) — `dict`,
  `Literal`, `Enum` and `Annotated` bounds had all been read as `"string"`.
- Stage ② is `Standardize`, stage ③ is `Answers`. The agent layer is one
  package (`mirobody/agent/`); routers are `mirobody/server/routers/`.
- `pulse/CLAUDE.md` was shipping to PyPI; it is `pulse/README.md`. Long-form
  guides moved to `docs/`; bare `pytest` runs the whole suite.

### Removed

- The `/charts` mount, the remote-config fetch (`CONFIG_SERVER`), dead config
  keys (`FILE_ANALYSIS_PROVIDER`, `VITAL_*`, `RENPHO_*`), and ~1,100 lines of
  verified-dead code.
