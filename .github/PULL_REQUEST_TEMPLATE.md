## What this changes

<!-- One or two sentences. If it fixes an issue, "Fixes #NN". -->

## Which gate you ran

<!-- Name the command and paste the counts. The suite has two roots and the
     numbers differ; say which you ran. -->

- [ ] `pytest -q` (full local suite, both roots)
- [ ] `pytest -q mirobody` (what a clone runs)
- [ ] `lint-imports` — required for anything touching `mirobody/kernel`, `mirobody/engine/` or `agent/`
- [ ] `uvx ruff check mirobody examples`
- [ ] `benchmarks/run_eval.py --no-embed` — required for any resolver or bundle change

## Which benchmark it moves

<!-- A resolver change moves coverage/wrong-rate; say the before and after.
     "None" is a fine answer for everything else. -->
