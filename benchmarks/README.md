# Benchmarks

Public and runnable from a clone: `pip install -e '.[app,test]'`, then the
commands below. Nothing here reads private data. Each suite has its own
`README.md` with the full detail; this page is the index.

| Suite | Command | Proves |
| --- | --- | --- |
| [`health_records/`](health_records/README.md) | `python -m unittest benchmarks.health_records.test_cases` | synthetic LOINC/UCUM readings and ICPC-3 complaint phrases resolve, in five languages, without acquiring a guessed code for an ambiguous or panel term |
| [`genomics/`](genomics/README.md) | `python -m unittest discover -s benchmarks/genomics -p 'test_*.py'` | the public genotype site catalog builds reproducibly from pinned public NCBI/1000G/PGP/CPIC sources and the packaged parser fixtures decode correctly |
| [`local_agent/`](local_agent/README.md) | `python benchmarks/local_agent/compare_mimo_bonsai.py --base <endpoint> --model <name>` | a local OpenAI-compatible chat model can drive the real `query_health_indicators` tool schema on six synthetic questions |
| `run_eval.py` | `python benchmarks/run_eval.py --testset <cases.jsonl>` | scores every resolver tier and fusion, by coverage/precision/recall/wrong-rate, against a test set **you** supply |

Measured from this worktree, `pip install -e '.[app,test]'` already active:

```
python -m unittest benchmarks.health_records.test_cases        # Ran 3 tests: OK
python -m unittest discover -s benchmarks/genomics -p 'test_*.py'  # Ran 34 tests: OK (skipped=2)
```

The two genomics skips want an external corpus (`MIROBODY_PUBLIC_PGP_DIR`, a
local CPIC SQL dump) that is not part of a clone; they are optional
integration checks, not failures.

`local_agent` needs a running chat endpoint (a local `llama-server` or
anything OpenAI-compatible) and is not a pass/fail gate: it records tool
calls, arguments and latency for a human to read against the prompt's rules.

`run_eval.py` is the harness, not a test: it ships the four metrics and the
grading rule, but the maintained test set is gitignored and not in this
repository (see its docstring). Point `--testset` at your own JSONL to score
the resolver against your own cases; nothing here is read unless you pass it.

## Changing the resolver

A resolver change belongs to two files: `mirobody/res/loinc/resolver_overrides.tsv`
(what the resolver reads at runtime) and `mirobody/res/loinc/aliases_src/`
(curated inputs to the bundle build). Either is judged by
[`mirobody/tests/test_engine_coverage.py`](../mirobody/tests/test_engine_coverage.py),
the public coverage gate: a term that misses is a one-line row in
`resolver_overrides.tsv` plus one case in that test file, run with

```bash
pytest mirobody/tests/test_engine_coverage.py -s
```
