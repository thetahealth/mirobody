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
| `'.[test]'` | 19 | 658 passed, 178 skipped, 12 xfailed |
| `'.[test,parse]'` | 77 | 707 passed, 176 skipped, 12 xfailed |
| `'.[test,app]'` | 144 | 946 passed, 113 skipped, 12 xfailed |

The extras change less of what a clone can run than the package counts
suggest. Twenty-nine public modules are kept in `mirobody/tests`, described
below, and most need no extra, so every install runs them in a clone.
`[parse]` adds 49 tests, almost all of them the capability probes (48), which
import the parser's dependencies. `[app]` adds 239 more, the ones that need
FastAPI or the MCP SDK and skip without them (`pytest.importorskip`): the
security gates (81), the extraction (70), collection (55) and plugin-directory
(22) regressions, the trend service (6) and the confirmation route checks (5).
The 113 database cases skip on every install
until `MIROBODY_TEST_POSTGRES_DSN` names a database (see below); CI supplies
one. The skills module also ships in the wheel as inspectable release
evidence, but skips when the wheel is installed without the repository-root
`skills/` directory. The 12 xfails are the known cross-language splits,
described below. What the extras still decide is what the SERVER needs, which
is what the package counts are for.

<sub>Measured 2026-10-27 for 1.5.9 on a clone-shaped tree, each install in a
fresh venv: packages are `uv pip freeze` lines, tests `pytest -q mirobody/tests`
(1071 collected with `[app]`; plain `pytest -q`, which also collects the
published modules under `tests/`, finds 1290). The table counts
`mirobody/tests` only; the published Python-analysis checks below live in
`tests/`. `pytest` in a checkout with the remaining maintainers' local
suite collects additional tests.</sub>

The published Python-analysis evidence adds 24 tests in
[`tests/test_code_tool_capability.py`](../tests/test_code_tool_capability.py)
and [`tests/agent/test_code_tool_probe.py`](../tests/agent/test_code_tool_probe.py).
Collection skips them when the agent/Docker SDK dependencies are absent;
three execution cases also need a local daemon and prepared Python image.
The 2026-10-21 source run with `[app,test]` and that image recorded
**645 passed, 106 skipped, 12 xfailed** across the public modules and these
two evidence modules. This excludes unrelated, untracked local tests.

The thinking/100-question integration has additional synthetic contracts in
`tests/agent/test_thinking_stream.py`, `tests/agent/test_chat_thinking_policy.py`
and `tests/agent/test_local_eval_contract.py`. `tests/agent/test_long_context.py`
triggers at least two genuine automatic DeepAgents summaries through the
production middleware and checkpoint; its scripted model can only retain
facts present in the actual summary input. `test_long_context_runner.py`
checks that the live stress runner rejects an unexercised compaction path.
These tests exercise the real compiled
harness with scripted models, provider request construction, source/case
fingerprints, answer bindings, medication history and SSE latency accounting.
They call no paid model. The tests use the frozen manifests and invented
fixtures; `cases.py --corpus <build> --check` separately verifies the actual
generator build's input hashes. Those checks are not model validation.
The full 100-question model evaluation and its setup are described in
[`benchmarks/local_models/README.md`](../benchmarks/local_models/README.md).

They used to abort collection outright rather than skip — first with
`ModuleNotFoundError: langchain_core`, then with `psycopg_pool` and `mandrill`
because `mirobody/server/__init__` and `mirobody/user/__init__` import them,
and in 1.3.0 with `dotenv` and `ruamel` after those left the base install. In every case the failing import sits at the top of the
test module, so a module-level `importorskip` is already too late;
`conftest.py` decides at COLLECTION time, which is the only point early
enough.

That is the entire happy path. `testpaths` is set, so bare `pytest` collects
two trees, and a third that is neither:

- `mirobody/tests/` — thirteen public modules, described below.
  It ships in the repository and is what `pytest mirobody` runs in a clone;
  only `test_skills.py` is carried into the wheel, and
  `scripts/check_wheel_data.py` fails if another test member turns up there.
- `tests/` at the repo root — the maintainers' regression suite, one module per
  package module (`mirobody/kernel/series.py`, `mirobody/standardize/` and
  `mirobody/collect/observations.py` each have one). It is gitignored except
  for the two published Python-analysis evidence modules above. A clone
  contains those two modules; the remaining regression tests stay local.
- `benchmarks/`: three runnable suites (`health_records/`, `genomics/`,
  `local_agent/`) plus `run_eval.py`, the resolver scoring harness that runs
  against a test set you supply. None is library code: nothing imports it.
  See [`benchmarks/README.md`](../benchmarks/README.md) for the command and
  what each proves.

## Where tests live

**Two roots; the public evidence is in `mirobody/tests/` and the two published
Python-analysis modules under `tests/`.**

`mirobody/tests/test_engine_coverage.py` is the gate that ships. It is
*evidence* for a number the README prints and links: a benchmark nobody can run
is an assertion, so the resolver score goes in the repository where anyone with
a clone can re-run it.

| Module | Covers | Notes |
| --- | --- | --- |
| `test_engine_coverage.py` | **the published accuracy number** | 345 cases: the panels an ordinary checkup prints, in English, 简体中文, 繁體中文, 日本語, Russian, Estonian and Turkish, plus device vocabulary, report shapes (`名称(缩写)`, `Name-ABBREV`, snake_case, full-width), unit-dependent codes and non-numeric readings. Run with `-s` to print the score; `COVERAGE_FLOOR = 1.0` |
| `test_cross_language_identity.py` | one analyte, one code, in every language | `SAME_ANALYTE` rows must agree; `KNOWN_SPLITS` are the rows that do not yet, each a strict xfail that fails once fixed, so the row moves up |
| `test_skills.py` | agent skill commands, frontmatter, Compose names and resolver examples | ships in the wheel and sdist as public evidence; it skips the repository-only skill files when run from `site-packages` |
| `test_security_gates.py` | the server's security decisions: the OAuth return URL, the second factor for a token in the query string, upload ownership, the placeholder `JWT_KEY`, the response headers | needs `[app]` and skips without it. Each case was reproduced on a running server before its fix |
| `test_resolver_regressions.py` | patient/object identity, singular/plural ranking and meaningful CamelCase qualifiers | library-only; failures name the input and the clinical axes |
| `test_collect_regressions.py` | Apple hub priority, report-printed H/L markers and VCF `loc:` citation identities | application cases skip safely on a minimal install |
| `test_confirmation_regressions.py` | confirmed aliases still need units; explicit units use observation amendments | pure coding checks run without `[app]`; route checks need it |
| `test_postgres_regressions.py` | actual schema, writer, confirmation, statistics, Apple import/re-import and citation lifecycle | needs `[app,test]` and `MIROBODY_TEST_POSTGRES_DSN`; CI supplies PostgreSQL 17 |
| `test_extraction_regressions.py` | signed values, range endpoints, unreadable exponents and unverified answer provenance | application tests; the database gate additionally checks all nine views, windows and elected inputs |
| `test_citations.py` | every number in an answer traced to the rows it cites (`kernel.citations`) | library-only; the agent, the benchmark and the training judge run the same check |
| `test_plugin_dir_regressions.py` | configured directories retain relative imports through symlinked packages and import roots; a checkout beside a non-editable install still reaches the installed copy, including below a namespace parent; plain directories keep their own files | library-only; toy packages under `tmp_path` |
| `test_local_model_contract.py` | the GPU and CPU presets agree on weights, context and the chat template | reads `docker/*.ini` and `docker/minicpm5-2b.jinja`; no model download |
| `test_windows_probe_regressions.py` | the Windows benchmark's speed probes fail on HTTP errors, unfinished streams and timeouts | synthetic local HTTP server; CI also runs it on `windows-latest` |
| `test_windows_scripts_regressions.py` | the Windows benchmark's shell orchestration, against stubbed commands | runs the scripts under `bash` with a stubbed `docker`; skips on Windows |

```bash
pytest mirobody/tests/test_engine_coverage.py -s
```

To run the database gate against a disposable PostgreSQL database:

```bash
MIROBODY_TEST_POSTGRES_DSN=postgresql://localhost/mirobody_regression \
  pytest -q mirobody/tests/test_postgres_regressions.py
```

The database user must be able to create schemas. Each case creates a unique
schema, loads the checked-in DDL, writes synthetic data and drops the schema
in `finally`. The fixtures' encryption functions are identity functions for
synthetic SQL round trips; encryption itself is covered by the security gates.
The connection searches only that schema, and background profile refresh is
disabled, so the fixture cannot fall through to another schema's application
tables or enqueue model work.
Without the DSN these database cases skip explicitly. With a DSN, missing
application dependencies or an unreachable database fail the run.

The daily-rollup aggregator has a maintainer script there too; it needs a
live database and a real user id, and the day-boundary rule it checks (a
night is filed on an 18:00-18:00 day, everything else on 00:00-24:00) is
recorded with its measurement in `mirobody/standardize/aggregate/windows.py`.

Everything else lives in the gitignored `tests/` at the repo root: the golden
LOINC codes, the units vectors, the gates on the READMEs' figures and links,
the export tables, the one-key provider matrix, and the authorization
regressions behind SECURITY.md. Those invariants are still guarded on every
change; they are simply not public surface. A module ships when outside
contributors need it to check their own change, and terminology rows are the
change they make.

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
is one row in `mirobody/res/loinc/resolver_overrides.tsv` plus one case here.

**Cross-language identity.** `test_cross_language_identity.py` is the other half
of a terminology change: the code is the series key, so two spellings of one
analyte that answer two codes put one person's readings on two trend lines. If
the analyte already has a spelling in another language, add yours to its row.

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
   there. CI fails on anything NEW, and the baseline may only shrink. With no
   tree arguments the command scans `phi_lint.DEFAULT_TREES`, the trees the
   baseline covers; regenerating over any other set writes a different file:

   ```bash
   python -m mirobody.testing.phi_lint --baseline mirobody/testing/phi_baseline.txt   # what CI runs
   python -m mirobody.testing.phi_lint --write-baseline                               # after removing some
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
MIROBODY_UPDATE_GOLDEN=1 pytest tests/standardize/test_indicators_info.py
```

A behaviour change is then a visible diff in review, never a silent
re-baseline.

## Checking the health-data path against a real database

The unit tests pin the tool's contract without a store and the SQL's shape
without a connection. Two failures only a database shows — a statement that
will not parse, and a column that is not there — so there is a third check:

```bash
docker compose exec -T mirobody python - --user <your account id> < scripts/e2e_health_data.py
```

15 cases and 4 invariants, including **"one day, one number"**: a `day` bucket
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
| standardize has one front door | and neither does `collect` reach past `mirobody.standardize` |

The third is regenerated from `pyproject.toml`'s own dependency lists by the
local suite, so adding a dependency without adding it to the contract fails.
**A new library-layer module must be added to the first four contracts AND to
that suite's list of library modules**, or the test is green for the wrong
reason. The last two are the opposite shape: they forbid rather than permit, so
a new submodule under `collect/` or `standardize/` is covered the moment its
parent is listed.

`lint-imports` must analyse the repo source: run it from a venv with this repo
installed editable. Inside a venv holding an older published wheel it passes
vacuously.

`check_wheel_data.py` exists because release 1.0.62 shipped a wheel whose data
files were 133-byte Git LFS pointers. `import mirobody` succeeded; every
`resolve()` failed. An import smoke test cannot catch that class of break, so
the gate inspects the built artifacts directly — both the wheel and the sdist,
since pip serves the sdist wherever the wheel doesn't match.
