# Testing

```bash
pip install -e '.[agents,test]'
pytest                    # the whole suite — 439 tests, ~9s, no DB, no network, no API key
```

`'.[test]'` without `[agents]` is a supported smaller install: it runs the
~165 engine tests and prints a header saying the agent-layer tests were skipped.
It used to abort collection outright with `ModuleNotFoundError: langchain_core`.

That is the entire happy path. `testpaths` is set, so bare `pytest` collects
`mirobody/**/test_*.py`.

> Earlier revisions of the README documented `pytest tests/ -m mcp` and friends.
> There has never been a top-level `tests/` directory in this repo and those
> markers were never defined, so that command collected zero tests while
> appearing to pass. If you find that instruction anywhere, it is stale.

## Where tests live

Beside the code they cover, not in a parallel tree — the test for
`mirobody/mcp/service.py` is `mirobody/mcp/test_protocol.py`. A module and its
test move together, and a reviewer sees both in one diff.

| Suite | Covers | Notes |
| --- | --- | --- |
| `mirobody/test_engine.py` | golden LOINC codes for ② Standardize | pins the whole chain: alias index → commonness prior → axis table |
| `mirobody/test_engine_coverage.py` | **the published accuracy number** | 175 cases: the panels a physical orders, in en/zh/ja, plus device vocabulary, report shapes (`名称(缩写)`, snake_case, full-width), unit-dependent codes and non-numeric readings. Prints the score; `COVERAGE_FLOOR = 1.0` |
| `mirobody/indicator/test_lexical.py` | the surface algebra | NFKC-lite folds, the CJK tokenizer, and the parenthetical split — including the ones it must REFUSE (`中性粒细胞(%)`) |
| `mirobody/indicator/test_semantic.py` | the opt-in semantic tier's contract | that `resolve()` never returns a semantic answer, that a refusal is not a miss, and that a width mismatch raises instead of padding. Uses a synthetic 3-row index: no matrix, no key |
| `mirobody/indicator/fhir/units/test_convert.py` | unit conversion | 31 golden vectors, half of them negative — BMI must not become a concentration, `%` must not become a count |
| `mirobody/server/routers/test_records_router.py` | the platform-shaped `/api` surface | wire shapes, the explicit-scope delete, and that `value` is not decrypted while `comment` is |
| `mirobody/mcp/test_protocol.py` | MCP wire behaviour | version negotiation, `resultType`, `server/discover`, and the cross-user JWT leak that `resources/read` once had |
| `mirobody/pulse/gate_tests/` | vendor payload → `StandardPulseData` | snapshot tests over recorded fixtures |
| `mirobody/pulse/aggregate/` | daily rollups, CGM indicators, source priority | the only suite that touches config |

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

**Resolver coverage.** `test_engine_coverage.py` is the number in the README. It grades *clinical* correctness rather than resolution rate: answering `血红蛋白` with the HbA1c code is a failure, and a panel name is required to resolve to nothing.
It grades *clinical* correctness, not resolution rate: answering `血红蛋白`
with the code for HbA1c is scored as a failure, and terms naming a panel
(`blood pressure`, `血脂`) are required to resolve to **nothing**. Adding a term
is one row in `mirobody/res/resolver_overrides.tsv` plus one case here.

```bash
pytest mirobody/test_engine_coverage.py -s     # prints the score and every miss
```

**Gate snapshots.** `pulse/gate_tests/` compares each vendor's real payload
against a recorded `StandardPulseData`. When the standard model gains a field,
every snapshot goes stale at once — that is expected, and the fix is:

```bash
pytest mirobody/pulse/gate_tests --update-snapshots
```

Read the diff before committing it. These 21 tests spent a long time dismissed
as "known failures on main" when in fact the fixtures simply predated
`metaInfo.windowFrom/windowTo`. A stale snapshot and a real regression look
identical until you check which side changed.

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
