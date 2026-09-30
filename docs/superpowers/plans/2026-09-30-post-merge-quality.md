# Post-Merge Quality and Release Surface Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans (inline execution is authorized by the user).

**Goal:** Make the merged 1.5.3 repository honest and shippable by publishing the approved skill test, correcting known category-word resolver errors, and tightening the public Docker Hub and GitHub metadata.

**Architecture:** Keep the resolver fix in its existing runtime override table so it applies to every lexical caller without introducing a new code path. Keep packaging narrow: retain only `mirobody/tests/test_skills.py` in the wheel, while the other checkout-only tests remain pruned. Put Docker Hub copy in a tracked source document and use the existing Docker Hub namespace and release workflow as the publication boundary.

**Tech Stack:** Python 3.12+, setuptools PEP 517 backend, pytest, GitHub CLI/API, Docker Hub v2 API.

## Global Constraints

- Run the five repository gates from `AGENTS.md` before claiming completion.
- Every resolver correction needs a failing regression assertion before the data change and a passing assertion after it.
- Keep `test_skills.py` outside the Python package's runtime imports, but include that file in release artifacts because it is public evidence for the skills.
- Keep no more than 20 GitHub repository topics; prefer discoverable health-data and agent terms over vendor-specific device terms.
- Do not put credentials in the repository, command output, or Docker Hub copy.

### Task 1: Verify the merged baseline and package-artifact requirement

**Files:**
- Modify: `scripts/build_backend.py`
- Modify: `scripts/check_wheel_data.py`
- Modify: `pyproject.toml`
- Modify: `docs/testing.md`
- Modify: `CHANGELOG.md`
- Test: `mirobody/tests/test_skills.py`

**Interfaces:**
- `_should_drop(member: str) -> bool` keeps `mirobody/tests/test_skills.py` and drops the other test artifacts.
- `check(dist/*.whl)` permits exactly the public skill gate module and still rejects all other package tests.

- [ ] Build a wheel from the clean merged tree and prove the current behavior is wrong: `python -m build --wheel --outdir /tmp/mirobody-dist-before`, then inspect the wheel for `mirobody/tests/test_skills.py`.
- [ ] Change the backend predicate to exempt only the normalized member path `mirobody/tests/test_skills.py`; update the wheel checker and documentation to state that one public evidence module ships.
- [ ] Build wheel and sdist, run `python scripts/check_wheel_data.py dist/*`, and inspect both artifacts for the test module.

### Task 2: Correct the six known category-word resolutions

**Files:**
- Modify: `mirobody/res/loinc/resolver_overrides.tsv`
- Modify: `mirobody/tests/test_skills.py`
- Modify: `skills/translate-health-data/reference.md`
- Modify: `skills/translate-health-data/SKILL.md`
- Modify: `CHANGELOG.md`

**Interfaces:**
- The existing `!unresolved` target in `resolver_overrides.tsv` is the single refusal mechanism used by `OfflineResolver._is_blocked`.
- The six terms `免疫`, `stool`, `重金属`, `heavy metals`, `激素`, and `enzymes` return `Resolution(method="refused", resolved=False)`.

- [ ] Replace the six `_KNOWN_BAD` positive assertions with refusal assertions and run them once to capture the expected failure before adding rows.
- [ ] Add exact English and Chinese refusal rows to the override table, preserving the longer specific rows already present.
- [ ] Update the skills reference from “wrong today” to “deliberately unresolved” and add the reason: these are category words that do not identify one LOINC observation.
- [ ] Run the focused resolver and skills tests, then the full five gates.

### Task 3: Public metadata and Docker Hub copy

**Files:**
- Create: `docs/docker-hub-description.md`
- Modify: `CHANGELOG.md`

**Interfaces:**
- The Docker Hub short description is one sentence under the repository character limit.
- The full description starts with the Docker quickstart, names the seven MCP tools and the two local/offline boundaries, and links to the docs site and GitHub repository.

- [ ] Set the repository topics to the final 20-topic list through GitHub's authenticated API.
- [ ] Record the Docker Hub short and full descriptions in the tracked source document, then publish them through Docker Hub only if a write-capable Docker Hub credential is available; otherwise report the exact manual/API step.
- [ ] Verify the public Docker Hub API returns the new description and the expected latest tag metadata.

### Task 4: Final verification and delivery

**Files:**
- No additional source files.

- [ ] Run `ruff check mirobody examples`.
- [ ] Run `python -m compileall -q mirobody`.
- [ ] Run `pytest -q`.
- [ ] Run `lint-imports`.
- [ ] Run `python3 -c "import mirobody.kernel.meds, mirobody.kernel.query"`.
- [ ] Build/check artifacts and verify the original bad terms no longer resolve to LOINC codes.
- [ ] Review `git diff`, `git status`, and the public metadata responses before reporting results.
