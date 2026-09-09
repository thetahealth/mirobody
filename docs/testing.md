# Testing

```bash
pip install -e '.[app,test]'
pytest                    # the whole suite — seconds, no DB, no network, no API key
```

`'.[test]'` alone is a supported smaller install: it runs the library tests
and prints a header naming the layers that were not installed. There are three
of them, and each abort was found the same way — in a clean clone, never in a
long-lived venv:

| install | packages | tests |
| --- | --- | --- |
| `'.[test]'` | 17 | 100 — resolve, units, lexical, the README gates |
| `'.[test,parse]'` | ~90 | 166 — + document extraction, model clients |
| `'.[test,app]'` | ~190 | 215 — everything |

They used to abort collection outright rather than skip — first with
`ModuleNotFoundError: langchain_core`, then with `psycopg_pool` and `mandrill`
because `mirobody/server/__init__` and `mirobody/user/__init__` import them,
and in 1.3.0 with `dotenv` and `ruamel` after those left the base install. In
every case a test module executes its parent package first, so a module-level
`importorskip` is too late; `conftest.py` decides at COLLECTION time, which is
the only point early enough.

That is the entire happy path. `testpaths` is set, so bare `pytest` collects
two trees, and a third that is neither:

- `tests/` at the repo root, mirroring the package: `tests/test_series.py` for
  `mirobody/kernel/series.py`, `tests/pulse/test_readings.py` for
  `mirobody/pulse/readings.py`, and so on, plus the repo-wide gates at the top
  (README figures and links, the public surface, the PHI baseline, the one-key
  defaults). Nothing under `mirobody/` is a test, so the wheel needs no
  pruning and `scripts/check_wheel_data.py` fails the build if a `test_*.py`
  ever appears inside it.
- `benchmarks/` — the resolver scoring harness (`run_eval.py`). Not a test
  (nothing asserts), not library code (nothing imports it); it runs from a
  checkout against a test set you point it at.

## Where tests live

**Two roots, and only one of them is published.**

`mirobody/` keeps the tests that are *evidence* for something this project
claims. A benchmark nobody can run is an assertion, so the resolver score the
README prints lives in the repository, next to the code it scores. The core
five:

| Suite | Covers | Notes |
| --- | --- | --- |
| `mirobody/test_engine_coverage.py` | **the published accuracy number** | the case table: the panels an ordinary checkup includes, in en / 简体中文 / 繁體中文 / 日本語, plus device vocabulary, report shapes (`名称(缩写)`, `Name-ABBREV`, snake_case, full-width), unit-dependent codes and non-numeric readings. Run with `-s` to print the score; `COVERAGE_FLOOR = 1.0` |
| `mirobody/test_engine.py` | golden LOINC codes for ② Translate | pins the whole chain: alias index → commonness prior → axis table |
| `mirobody/test_readme_numbers.py` | every figure the four READMEs publish | each one re-derived from the artifact or code that defines it, so a number cannot drift silently |
| `mirobody/test_readme_links.py` | every link and demo asset in the four READMEs | a dead relative link is a broken promise on the front page; localized GIFs must be referenced by their own translations |
| `mirobody/test_readme_l10n.py` | the translations themselves | script hygiene (no Simplified characters in 繁體中文), and every language shows the same demo |
| `mirobody/test_one_key_defaults.py` | the "one key runs everything" promise | the shipped defaults must chat, see and embed with a single OPENROUTER_API_KEY or DASHSCOPE_API_KEY, no overlay edits |
| `mirobody/test_public_surface.py` | every `__all__` in the package | a name left in one after the symbol is deleted turns `import *` into an `AttributeError` |

A second, smaller ring backs promises the docs make about *behavior*: the
care-circle isolation the front page demonstrates
(`mirobody/user/test_care_circle.py`, `mirobody/server/test_member_seed.py`), the
deployment posture SECURITY.md documents (`mirobody/server/test_bootstrap_guard.py`),
the CLI rendering the quickstart GIF shows (`mirobody/test_cli_width.py`), and a few
contracts whose docstrings are load-bearing (`mirobody/utils/test_content_type.py`,
`mirobody/user/test_user_lookup.py`, `tests/agent/filesystem/test_naming.py`,
`mirobody/pulse/file_parser/services/test_delete_is_deletion.py`).

## Markers

Two, both meaning "this test wants something the sandbox does not have":

```bash
pytest -m "not needs_db"     # skip anything wanting a live PostgreSQL
pytest -m "not needs_llm"    # skip anything that would call a paid model
```

Neither is needed for a normal run — nothing in the default suite requires a
database, the network, or an API key. That is deliberate: a contributor should
be able to clone, `pip install -e '.[test]'`, and get a green suite in seconds.

## The two tests that carry the project's claims

**Resolver coverage.** `test_engine_coverage.py` is the number in the README.
It grades *clinical* correctness, not resolution rate: answering `血红蛋白`
with the code for HbA1c is scored as a failure, and a category with no panel
code of its own (`血脂`, `lipid panel`) is required to resolve to **nothing**,
while a panel term that has one (`blood pressure` → `85354-9`) is required to
resolve to the panel and never to one of its members. Adding a term
is one row in `mirobody/res/resolver_overrides.tsv` plus one case here.

```bash
pytest mirobody/test_engine_coverage.py -s     # prints the score and every miss
```

**Gate snapshots** (maintainers' internal tree — not in a clone). The
gate tests compare each vendor's real payload against a recorded
`StandardPulseData`. When the standard model gains a field, every snapshot
goes stale at once — that is expected, and the fix is:

```bash
pytest tests/pulse/gate_tests --update-snapshots   # maintainers only
```

Read the diff before committing it. A stale snapshot and a real regression
look identical until you check which side changed.

## The toolbox a consumer can run against its own code

`mirobody.testing` is not this repository's test suite — it is what this
repository learned, packaged so a downstream project does not have to relearn
it. It ships in the wheel, imports nothing but the standard library, and never
imports `pytest`: every function returns findings, and only the `assert_*`
helpers raise.

| Module | Checks | Run it |
|---|---|---|
| `phi_lint` | log statements carry ids, counts, durations, status codes and type names — never a value | `python -m mirobody.testing.phi_lint <trees>` |
| `prompts` | a prompt template names only tools the harness actually provides | `lint_prompt(text, registry=..., excluded=..., allow=...)` |
| `samples` | vendor payloads decode to the facts a human computed by hand | `run_samples(decode, root)` |
| `contracts` | a `sink.Sink` implementation is idempotent and skips unchanged rows | `check_sink(sink)` |
| `coverage` | the published provider matrix is what the decoders produce | `gen_coverage`, `embed`, `stale` |
| `golden` | snapshots with an explicit update switch | `assert_golden_json(path, value)` |

### PHI, in three executable layers

1. **Static.** `phi_lint` walks the AST of every `logger.*` call and reports any
   interpolated expression whose SHAPE is not allowed. It is deliberately
   shape-based: it cannot prove a variable holds no health data, but it makes
   "log the whole tool result" impossible to write without an explicit
   `# phi: ok <reason>` escape.

   The baseline (`mirobody/testing/phi_baseline.txt`) records what was already
   there. `tests/test_phi_baseline.py` fails on anything NEW, and the
   baseline may only shrink:

   ```bash
   python -m mirobody.testing.phi_lint mirobody --write-baseline   # after removing some
   ```

2. **Runtime.** `ops.PHIPolicy().install()` adds a logging filter that redacts
   by field name, so a log line added at 2am is caught even if nobody ran the
   lint.

3. **End to end.** One demo reading's comment carries `demo.PHI_CANARY`, and
   the Docker check greps the running container's logs for it. A STRING, not
   an odd number: a leaked bare value is indistinguishable from any other
   number in a log, while a leaked comment is unambiguous — and a comment is
   free-text health data, the thing the column encryption exists to protect.
   A redaction that holds in unit tests and not in the container is not a
   redaction.

### Snapshots

`assert_golden_json` writes the file when it does not exist and FAILS, so a new
snapshot is reviewed before it becomes the baseline. Updating one is explicit:

```bash
MIROBODY_UPDATE_GOLDEN=1 pytest tests/pulse/standardize/test_indicators_info.py
```

A behaviour change is then a visible diff in review, never a silent
re-baseline.

## Checking the health-data path against a real database

The unit tests pin the tool's contract without a store and the SQL's shape
without a connection. Two failures only a database shows — a statement that
will not parse, and a column that is not there — so there is a third check:

```bash
docker compose exec mirobody python -m scripts.e2e_health_data --user 1
```

22 cases and 4 invariants, including **"one day, one number"**: a `day` bucket
and a `latest` over the same day must agree, because that is what "the chat
answer and the dashboard show the same thing" means when it is written down.
Exit status is the number of failures, so it is usable in CI.

`--capture DIR` writes each answer as JSON. That is how the behaviour of the
tool this replaced was recorded before the rewrite: run it against the old
code, keep the directory, diff it after.

## Gates beyond pytest

```bash
ruff check mirobody                                 # the lint gate; 0 findings on main
lint-imports                                        # FOUR contracts, machine-checked
python -m build && python scripts/check_wheel_data.py dist/*   # artifacts carry real data, not LFS stubs
python3 -c "import mirobody.kernel.meds, mirobody.kernel.query"   # the library layer, on a bare interpreter
```

The four import-linter contracts, and what each one is for:

| Contract | Says |
|---|---|
| engine is agent-framework-free | the data engine imports no langchain/langgraph/deepagents |
| the library layer stands alone | the kernel modules import each other and nothing else in the package |
| the library layer is stdlib + numpy | and no third-party distribution this project declares, except numpy |
| engine does not import the agent layer | no seams: the health profile moved to `user/profile.py` |

The third is regenerated from `pyproject.toml`'s own dependency lists by
`tests/test_library_layer.py`, so adding a dependency without adding it to
the contract fails. **A new library-layer module must be added to all four
contracts AND to `test_library_layer.py::LIBRARY_MODULES`**, or the test is
green for the wrong reason.

`lint-imports` must analyse the repo source: run it from a venv with this repo
installed editable. Inside a venv holding an older published wheel it passes
vacuously.

`check_wheel_data.py` exists because release 1.0.62 shipped a wheel whose data
files were 133-byte Git LFS pointers. `import mirobody` succeeded; every
`resolve()` failed. An import smoke test cannot catch that class of break, so
the gate inspects the built artifacts directly — both the wheel and the sdist,
since pip serves the sdist wherever the wheel doesn't match.
