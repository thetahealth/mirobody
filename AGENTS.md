# Working on Mirobody with a coding agent

Read this before touching the tree. It is the short version of
[CONTRIBUTING.md](CONTRIBUTING.md) plus the things an agent gets wrong first.

## What this is

A Python 3.12+ health-data engine in three layers, and the layering is
machine-checked (`lint-imports`, contracts in `pyproject.toml`):

| Layer | Where | Installs with | May import |
|---|---|---|---|
| ② Standardize + the kernel (the library) | vocabulary: `engine.py`, `lexical.py`, `units/`, `value_scale.py`, `zh_fold.py`, `_bundle.py`, `_strtab.py`; semantics: **`kernel/`** (`metrics`, `series`, `quality`, `overlay`, `meds`, `query`, `tools`, `ops`, `connect`, `sink`, `events`, `evidence`, `memory`, `vendors/`); toolbox: `testing/` | `pip install mirobody` (numpy only) | each other, nothing else |
| ① Collect + storage + MCP | `mirobody/documents/`, `pulse/`, `indicator/`, `utils/`, `user/`, `task/`, `mcp/` | `[parse]` / `[app]` | no `langchain*`, `langgraph`, `deepagents` |
| ③ Answers | `mirobody/agent/` (one agent: `MirobodyAgent`, on `deepagents`), `server/` | `[agent]` (the harness as a library) / `[app]` | anything |

There is one agent, and it is not switched at request time. `BaseAgent`,
the `agent/base` package and the ChatGPT Apps widgets under `agent/resources`
were removed; do not reintroduce a second harness or a per-agent config
suffix. A deployment that wants its own harness REPLACES `MirobodyAgent` by
pointing `AGENT_DIRS` at its own directory (`agent/registry.py`). External
clients get the engine through `mirobody/mcp/` (six tools: `resolve_indicator`,
`convert_unit`, `normalize_unit`, `query_health_indicators`, `query_medications`,
`query_genetic_data`) —
and that list is asserted exactly, in `tests/agent/test_tool_surface.py`.
The agent's config keys are `PROVIDERS`, `PROMPTS`, `ALLOWED_TOOLS`,
`DISALLOWED_TOOLS`, `DEFAULT_PROVIDER`, `AGENT_NAME` — no suffix.

## Setup that actually works from a source clone

```bash
git lfs install && git lfs pull         # data bundles; without this `resolve` gets pointer stubs
python -m venv .venv && . .venv/bin/activate
pip install -e '.[app,test]'
```

The extras are `[parse]`, `[agent]`, `[app]`, `[test]` and `[indicator-build]`.
`[agents]` — plural — has never existed and is not the same thing as `[agent]`,
which 1.4.0 added: pip only WARNS about an unknown extra, so `-e '.[agents,test]'`
quietly installed `[test]` alone, which is how both CI workflows spent a release
running the minimal suite while reporting the full one. If a doc says `[agents]`,
`[server]` or `[cn]`, the doc is wrong.

## The gates — run all four before you say "done"

```bash
ruff check mirobody examples   # rule set in pyproject.toml; 0 findings on main
python -m compileall -q mirobody
pytest -q               # 305 tests on a clone with [app,test]; 189 (+16 skipped)
                        # on [test] alone, fewer by design, and the header says which
lint-imports            # 4 contracts, must say "0 broken"
python3 -c "import mirobody.kernel.meds, mirobody.kernel.query"   # the library layer, bare interpreter
```

With a live database, one more — it catches the two things a unit test cannot,
a statement that will not parse and a column that is not there:

```bash
docker compose exec mirobody python -m scripts.e2e_health_data --user 1
```

`lint-imports` and `pytest` must run against the repo source, not an installed
wheel in the same venv — otherwise they pass vacuously.

## Rules that are not the defaults you would assume

- **No compatibility shims, no `_v2` suffixes, no "kept for backward
  compatibility".** Rename or delete; `git` remembers. When you consolidate two
  implementations, confirm they are behaviour-identical or name the difference
  in the commit and the CHANGELOG.
- **Comments say why, with evidence; never what.** A stale comment is a bug
  and part of your change. English only, quoting non-English data is fine.
- **Verify, don't reason.** Before deleting "unused" code compute reachability
  transitively (a sibling may call it). Before repeating a claim from a README,
  run the command.
- **Every `th_series_data` write goes through `pulse/readings.py`.** Every
  FastAPI router answers with `server/envelope.py`. Every read of a person's
  readings goes through `query.HealthQuery`. Don't add a sixth INSERT, a fourth
  envelope, or a second copy of the query — when there were two, the chat
  answer and the dashboard could disagree about the same Tuesday.
- **A new LIBRARY-LAYER module goes under `mirobody/kernel/`, into all FOUR
  import-linter contracts AND into `tests/test_library_layer.py::LIBRARY_MODULES`.** Miss either and the gate is
  green for the wrong reason. It may import stdlib, numpy and its siblings —
  nothing else — and it must not contain a `test_*.py` (a test inside the
  package drags pytest into the library layer).
- **Logs carry ids, counts, durations, status codes and type names. Never a
  value.** `tests/test_phi_baseline.py` fails on anything new; the baseline
  may only shrink. An indicator NAME is not a value but it is still the answer
  to "what was measured", so it does not go in either. In a broad `except`,
  `exc_info=not is_driver_exception(e)` — a driver's message quotes the SQL
  with its bound parameters.
- **A prompt may only name tools the harness provides** (`test_prompts.py`).
  Renaming a tool means the template and that registry change in the same
  commit.
- **A decode table says how to convert INTO the catalogue's unit; it never
  writes the unit down.** The unit is `metrics.METRICS[name].unit_ucum`, so a
  decoder cannot disagree with the aggregator.
- **Nothing derived is stored.** A dose's `due`/`missed`/`upcoming` come from
  the clock (`meds.slot_state`); adherence is counted, never persisted. A
  stored `missed` is a lie the moment the person marks the dose taken.
- **Logging:** `logger = logging.getLogger(__name__)` at module top; never
  `logging.info(...)` on the root logger; no emoji in log lines.
- **Provider contract:** `format_data(self, fmt_input: FormatDataInput) ->
  StandardPulseData` and `_validate_credentials(self, credentials: dict)`.
  Guide: `docs/provider-guide.md`.
- **Docs are code.** Rename a module → grep the `.md` files. The README ships
  in four languages; English first, then the other three, and say so if you
  only changed English. `mirobody/test_readme_*.py` checks them as a set.

## Never commit

- `.env` and anything holding a key (the repo's `.gitignore` covers `.env`,
  `.venv*/`, `internal/`).
- Real health documents used for local testing. Synthetic fixtures live under
  the repo-root `demo/`; anything with a person's data stays outside the tree.

## Where things are decided

- CHANGELOG.md → "Unreleased" gets an entry for every user-visible change,
  written as *what was wrong, what changed, how to tell*.
- `docs/roadmap.md` → known defects and open design questions; close an item
  there when you fix it.
- `internal/` is gitignored planning material; nothing there is a promise.
- `docs/pipeline.md` → the eleven stages and the ten invariants, each with the
  failure it prevents. Read it before changing anything between a payload and a
  stored row.
- `docs/answers.md` → the tool surface, the applicability matrix, the envelope
  and what stops a model looping.
- `docs/medications.md` → the medication model, its state tables and its
  grammar. Marked `provisional` until a second production consumer exists.
