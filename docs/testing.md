# Testing

```bash
pip install -e '.[agents,test]'
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
`mirobody/**/test_*.py`.

> `tests/` at the repo root is the maintainers' internal tree — config
> hygiene, the no-magic-names scan, gate-fixture honesty: cross-package
> regression guards for problems the 2026-08 review exposed. It is **not
> published**; a clone does not contain it, and bare `pytest` neither needs
> nor misses it. Unit tests still live next to their code.
>
> Earlier revisions of the README documented `pytest tests/ -m mcp` and friends.
> At the time there was no top-level `tests/` directory in this repo and those
> markers were never defined, so that command collected zero tests while
> appearing to pass. If you find that instruction anywhere, it is stale.

## Where tests live

**Two roots, and only one of them is published.**

`mirobody/` keeps the tests that are *evidence* for something this project
claims. A benchmark nobody can run is an assertion, so the resolver score the
README prints lives in the repository, next to the code it scores. The core
five:

| Suite | Covers | Notes |
| --- | --- | --- |
| `mirobody/test_engine_coverage.py` | **the published accuracy number** | the case table: the panels an ordinary checkup includes, in en / 简体中文 / 繁體中文 / 日本語, plus device vocabulary, report shapes (`名称(缩写)`, `Name-ABBREV`, snake_case, full-width), unit-dependent codes and non-numeric readings. Run with `-s` to print the score; `COVERAGE_FLOOR = 1.0` |
| `mirobody/test_engine.py` | golden LOINC codes for ② Standardize | pins the whole chain: alias index → commonness prior → axis table |
| `mirobody/test_readme_numbers.py` | every figure the four READMEs publish | each one re-derived from the artifact or code that defines it, so a number cannot drift silently |
| `mirobody/test_readme_links.py` | every link and demo asset in the four READMEs | a dead relative link is a broken promise on the front page; localized GIFs must be referenced by their own translations |
| `mirobody/test_readme_l10n.py` | the translations themselves | script hygiene (no Simplified characters in 繁體中文), and every language shows the same demo |
| `mirobody/test_one_key_defaults.py` | the "one key runs everything" promise | the shipped defaults must chat, see and embed with a single OPENROUTER_API_KEY or DASHSCOPE_API_KEY, no overlay edits |
| `mirobody/test_public_surface.py` | every `__all__` in the package | a name left in one after the symbol is deleted turns `import *` into an `AttributeError` |

A second, smaller ring backs promises the docs make about *behavior*: the
care-circle isolation the front page demonstrates
(`user/test_care_circle.py`, `demo/test_member_seed.py`), the deployment
posture SECURITY.md documents (`server/test_bootstrap_guard.py`), the CLI
rendering the quickstart GIF shows (`test_cli_width.py`), and a few contracts
whose docstrings are load-bearing (`utils/test_content_type.py`,
`user/test_user_lookup.py`, `agent/deep/test_filetype.py`,
`pulse/file_parser/services/test_delete_is_deletion.py`).

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

## Gates beyond pytest

```bash
lint-imports                                        # the engine/agent boundary, machine-checked
python -m build && python scripts/check_wheel_data.py dist/*   # artifacts carry real data, not LFS stubs
```

`lint-imports` must analyse the repo source: run it from a venv with this repo
installed editable. Inside a venv holding an older published wheel it passes
vacuously.

`check_wheel_data.py` exists because release 1.0.62 shipped a wheel whose data
files were 133-byte Git LFS pointers. `import mirobody` succeeded; every
`resolve()` failed. An import smoke test cannot catch that class of break, so
the gate inspects the built artifacts directly — both the wheel and the sdist,
since pip serves the sdist wherever the wheel doesn't match.
