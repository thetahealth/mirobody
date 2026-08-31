# Contributing to Mirobody

Thank you for your interest in contributing to Mirobody! We welcome contributions from the community to help make this project better.

## 🤝 How to Contribute

### Reporting Bugs
If you find a bug, please create a new issue on GitHub. include:
- A clear title and description.
- Steps to reproduce the issue.
- Your environment details (OS, Docker version, etc.).

### Suggesting Features
We love new ideas! Please open an issue to discuss your feature idea before implementing it. This helps ensure your time is well spent and the feature aligns with the project's goals.

## 🛠️ Development Workflow

1.  **Fork the Repository**
    Click the "Fork" button on the top right of the repository page.

2.  **Clone Your Fork**
    ```bash
    git clone https://github.com/YOUR_USERNAME/mirobody.git
    cd mirobody
    ```

3.  **Create a Branch**
    Create a new branch for your feature or fix:
    ```bash
    git checkout -b feature/my-new-feature
    # or
    git checkout -b fix/bug-fix-name
    ```

4.  **Make Changes**
    - Follow the existing code style.
    - Write clear and concise commit messages.

5.  **Test Your Changes**
    `deploy.sh` used to be listed here. It builds and starts the Docker stack and
    runs no tests at all, so following this step told you nothing about whether
    you had broken something. The real gates:

    ```bash
    pip install -e '.[agents,test]'    # everything

    pytest                # the tests this repo ships: the resolver
                          # benchmark and the README gates
    lint-imports          # the engine/agent boundary, machine-checked
    ```

    `'.[test]'` alone is enough to work on the **library** — resolve, units,
    lexical. On a clean clone that is 17 packages and runs 100 tests, printing
    a header naming what it skipped. Add `[parse]` for the document-extraction
    and model-client tests (166), and `[app]` for the server and agent layers
    (the full 215). All three layers are dropped at COLLECTION time rather
    than aborting the run: a module-level `importorskip` is too late, because
    importing a test module imports its parent package first and that is what
    pulls in the missing dependency.

    `lint-imports` must run against the repo source — inside a venv holding an
    installed older wheel it passes vacuously.

    If you touched packaging or the shipped data bundles, also:

    ```bash
    python -m build && python scripts/check_wheel_data.py dist/*
    ```

    which rejects a wheel whose data files are missing or are Git-LFS pointer
    stubs. That gate exists because we published wheels that imported fine and
    could not resolve anything.

6.  **Push and Pull Request**
    Push your branch to your fork:
    ```bash
    git push origin feature/my-new-feature
    ```
    Then, open a Pull Request (PR) against the `main` branch of the original repository.

## 📝 Coding Style

This codebase was written by many hands over a long time. These rules are what
we converged on; they are worth reading once because several are not the
defaults you might assume.

### Priorities, in order

1. **Correct.** A wrong answer delivered confidently is the worst outcome this
   project can produce — it is health data.
2. **Testable.** Prefer a function that takes its inputs to one that reaches for
   global config at call time. If you cannot write a test for it without a
   database, say why in the docstring.
3. **Concise and precise**, in that order after the first two. Do not trade
   correctness for brevity.

Concretely: no backwards-compatibility shims, no defensive validation for states
that cannot occur, no abstraction built for one caller. Delete dead code rather
than renaming or commenting it out — `git` remembers.

### Comments

**English, always** — including comments that quote non-English data. Quoting is
encouraged: `# ``血气分析`` panels operate on arterial blood` is a *good*
comment, because the term is what the code matches.

**Explain why, with evidence. Never restate the code.**

```python
# Bad — says what the next line already says
# Get the user id
user_id = request.state.user_id

# Good — records the bug that motivated the line
# `next(..., None)` rather than `[...][0]`: this and the nearby rows come from
# two separate queries, so the pair is not guaranteed to still be present. The
# bare index raised IndexError and lost the whole response.
query_rsid = next((r.get("rsid") for r in result if ...), None)
```

A comment naming the failure it prevents survives refactors. A comment
paraphrasing the line does not, and becomes a lie the first time the line
changes.

**A stale comment is a bug.** If you change behaviour, the comments describing it
are part of the change. We have shipped comments that confidently described
behaviour the code had not had for a year.

### Docstrings

Module docstrings say what the module is *and what it is not* when confusion is
likely — e.g. `utils/crypto.py` states it is AES-GCM for stored values, not the
Fernet encrypter in `utils/config/encrypt.py`. Public functions document the contract
callers depend on, not the implementation.

### Verify before you assert

The rule that has caught the most real bugs here: **do not reason from what the
code appears to do — run it.**

- Before deleting something as unused, compute reachability *transitively*. A
  helper with no external callers may be called by a live sibling.
- Before consolidating two implementations, confirm they are behaviour-
  identical, not merely similar. Different fallbacks mean it is a behaviour
  change, and it needs its own commit and its own test.
- Before repeating a claim from our own docs, test it. Our README has been wrong.

### Documentation

Docs must match the code. A command in a README is a promise that it runs —
we have shipped a documented subcommand that exits with `invalid choice`. If you
rename a module, grep the `.md` files.

See [`docs/README.md`](docs/README.md) for which file a given piece of
documentation belongs in.

### Translations

The README ships in English, 简体中文, 繁體中文 and 日本語; the web client ships
the same four. **English is the source of truth** — change it first, then the
others, and if you only change English say so in the PR so the drift is visible
instead of silent.

Two rules that are specific to this project:

- **A translation is not a character conversion.** 繁體中文 is written for
  Taiwan usage, and that is word choice, not glyphs: 資料/檔案/登入, and — this
  is the one that matters — **血紅素** for haemoglobin, not 血紅蛋白. Converting
  the Simplified form character by character gives 血红素, which the raw index
  answers with the code for **HbA1c**. We shipped exactly that bug; see
  `res/resolver_overrides.tsv`.
- **Medical terms come off a real report**, not from a dictionary: a 体检报告
  (mainland), a 檢驗報告單 (Taiwan), a 健康診断結果表 (Japan). Triglycerides are
  甘油三酯 / 三酸甘油酯 / 中性脂肪 in the three, and only the last is what a
  Japanese 健診 form actually prints.

Adding a new indicator term in any language is one row in
`mirobody/res/resolver_overrides.tsv` plus one case in
`mirobody/test_engine_coverage.py`. The right-hand side of an override row must
be a key the alias index can look up — **not another row's left-hand side**; the
resolver does not follow a two-hop chain, and such a row resolves to nothing
while looking correct.

UI strings are **not** in this repo. `frontend/` holds the built web client, not
its source, so there is no `i18n/` here to edit. What is translatable here is the
four READMEs — `README.md` plus `.zh-CN` / `.zh-TW` / `.ja` — and they are checked
as a set by `mirobody/test_readme_links.py` and `mirobody/test_readme_numbers.py`.
Everything under `docs/` and every in-package `README.md` stays English.

### Commits

Explain *why*, and state how the change was verified. "fix bug" tells a future
reader nothing; "the handler caught its own 404 and returned HTTP 200" tells them
everything.

## ⚖️ License
By contributing, you agree that your contributions will be licensed under the project's [LICENSE](./LICENSE).
