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
| `'.[test]'` | 17 | 33 passed |
| `'.[test,parse]'` | 74 | 33 passed |
| `'.[test,app]'` | 146 | 33 passed |

The extras no longer change what a clone can run, and that is not a mistake in
the table. Since 1.4.3 one gate module ships (`test_engine_coverage.py`, the
resolver score the README links); the other twenty joined the maintainers'
local suite. That module needs no extras, so all three installs run it and
nothing else. What the extras still decide is what the SERVER needs, which is
what the package counts are for.

<sub>Measured 2026-09-14 on a clone-shaped tree (1.4.2). `pytest` in a checkout
that also has the maintainers' local suite collects more; these are the numbers
a clone sees.</sub>

They used to abort collection outright rather than skip — first with
`ModuleNotFoundError: langchain_core`, then with `psycopg_pool` and `mandrill`
because `mirobody/server/__init__` and `mirobody/user/__init__` import them,
and in 1.3.0 with `dotenv` and `ruamel` after those left the base install. In every case the failing import sits at the top of the
test module, so a module-level `importorskip` is already too late;
`conftest.py` decides at COLLECTION time, which is the only point early
enough.

That is the entire happy path. `testpaths` is set, so bare `pytest` collects
two trees, and a third that is neither:

- `mirobody/tests/` — one module, `test_engine_coverage.py`, described below.
  It ships in the repository and is what `pytest mirobody` runs in a clone; the
  build prunes the directory, and `scripts/check_wheel_data.py` fails if a
  member of it turns up in the wheel.
- `tests/` at the repo root — the maintainers' regression suite, one module per
  package module (`mirobody/kernel/series.py`, `mirobody/translate/` and
  `mirobody/collect/observations.py` each have one). It is gitignored, so it is
  simply absent from a clone, and pytest skips a testpath that does not exist.
  Nothing in this document names a file inside it: a clone cannot open one.
- `benchmarks/` — the resolver scoring harness (`run_eval.py`). Not a test
  (nothing asserts), not library code (nothing imports it); it runs from a
  checkout against a test set you point it at.

## Where tests live

**Two roots, and only one module of one of them is published.**

`mirobody/tests/test_engine_coverage.py` is the gate that ships. It is
*evidence* for a number the README prints and links: a benchmark nobody can run
is an assertion, so the resolver score goes in the repository where anyone with
a clone can re-run it.

| Module | Covers | Notes |
| --- | --- | --- |
| `test_engine_coverage.py` | **the published accuracy number** | 213 cases: the panels an ordinary checkup prints, in English, 简体中文, 繁體中文 and 日本語, plus device vocabulary, report shapes (`名称(缩写)`, `Name-ABBREV`, snake_case, full-width), unit-dependent codes and non-numeric readings. Run with `-s` to print the score; `COVERAGE_FLOOR = 1.0` |

```bash
pytest mirobody/tests/test_engine_coverage.py -s
```

The daily-rollup aggregator has a maintainer script there too; it needs a
live database and a real user id, and the day-boundary rule it checks (a
night is filed on an 18:00-18:00 day, everything else on 00:00-24:00) is
recorded with its measurement in `mirobody/translate/aggregate/windows.py`.

Everything else lives in the gitignored `tests/` at the repo root: the golden
LOINC codes, the units vectors, the gates on the READMEs' figures and links,
the export tables, the one-key provider matrix, and the authorization
regressions behind SECURITY.md. Those invariants are still guarded on every
change; they are simply not public surface. This is a young project whose
readers file issues rather than patches, and a suite shipped for contributors
who are not there yet is scaffolding, not evidence.

If you are working from a clone and want a regression test to come with your
change, put it anywhere under `tests/` and say so in the issue or PR. A
maintainer folds it into the suite.

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
pytest mirobody/tests/test_engine_coverage.py -s     # prints the score and every miss
```

**Gate snapshots** (maintainers' internal tree — not in a clone). The
gate tests compare each vendor's real payload against a recorded
`StandardPulseData`. When the standard model gains a field, every snapshot
goes stale at once — that is expected, and the fix is:

```bash
pytest tests/collect/gate_tests --update-snapshots   # maintainers only
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
   there. The local suite fails on anything NEW, and the baseline may only
   shrink:

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
MIROBODY_UPDATE_GOLDEN=1 pytest tests/translate/test_indicators_info.py
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

16 cases and 4 invariants, including **"one day, one number"**: a `day` bucket
and a `latest` over the same day must agree, because that is what "the chat
answer and the dashboard show the same thing" means when it is written down.
Exit status is the number of failures, so it is usable in CI.

`--capture DIR` writes each answer as JSON. That is how the behaviour of the
tool this replaced was recorded before the rewrite: run it against the old
code, keep the directory, diff it after.

## Gates beyond pytest

```bash
ruff check mirobody                                 # the lint gate; 0 findings on main
lint-imports                                        # SIX contracts, machine-checked
python -m build && python scripts/check_wheel_data.py dist/*   # artifacts carry real data, not LFS stubs
python3 -c "import mirobody.kernel.meds, mirobody.kernel.query"   # the library layer, on a bare interpreter
```

The six import-linter contracts, and what each one is for:

| Contract | Says |
|---|---|
| engine is agent-framework-free | the data engine imports no langchain/langgraph/deepagents |
| the library layer stands alone | the kernel modules import each other and nothing else in the package |
| the library layer is stdlib + numpy | and no third-party distribution this project declares, except numpy |
| engine does not import the agent layer | no seams: the health profile moved to `user/profile.py` |
| collect has one front door | `agent` and `server` import `mirobody.collect`, never its submodules |
| translate has one front door | and neither does `collect` reach past `mirobody.translate` |

The third is regenerated from `pyproject.toml`'s own dependency lists by the
local suite, so adding a dependency without adding it to the contract fails.
**A new library-layer module must be added to the first four contracts AND to
that suite's list of library modules**, or the test is green for the wrong
reason. The last two are the opposite shape: they forbid rather than permit, so
a new submodule under `collect/` or `translate/` is covered the moment its
parent is listed.

`lint-imports` must analyse the repo source: run it from a venv with this repo
installed editable. Inside a venv holding an older published wheel it passes
vacuously.

`check_wheel_data.py` exists because release 1.0.62 shipped a wheel whose data
files were 133-byte Git LFS pointers. `import mirobody` succeeded; every
`resolve()` failed. An import smoke test cannot catch that class of break, so
the gate inspects the built artifacts directly — both the wheel and the sdist,
since pip serves the sdist wherever the wheel doesn't match.
