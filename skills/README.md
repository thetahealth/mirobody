# Skills

Skills for someone else's agent: Claude Code, Codex, Cursor, Gemini CLI,
OpenClaw and anything else that reads a `SKILL.md`. Each directory is one
skill, installable on its own:

```bash
npx skills add thetahealth/mirobody --skill <name>
```

Or install both without Node, from the plugin marketplace
(`.claude-plugin/marketplace.json`). Codex reads that same file, so one
manifest serves both:

```bash
claude plugin marketplace add thetahealth/mirobody && claude plugin install mirobody@mirobody
codex plugin marketplace add thetahealth/mirobody && codex plugin add mirobody@mirobody
```

Inside a Claude Code session the same two steps are
`/plugin marketplace add thetahealth/mirobody` and
`/plugin install mirobody@mirobody`. Claude Code warns that two files arrived
as Git LFS pointers. They are the LOINC and CPIC data bundles, which neither
skill reads: the resolver comes from PyPI.

| Skill | For | Needs |
| --- | --- | --- |
| [`translate-health-data`](translate-health-data/) | Raw health data into coded rows, and reading a lab report without guessing: lab documents, an Apple Health export, symptoms, units, FHIR | `pip install mirobody` on Python 3.12+; a key only to parse documents |
| [`mirobody`](mirobody/) | Running the whole engine and connecting an agent to it over MCP | Docker, one model key |

Every code, command and number the skills print is a promise made to a
stranger with no way to check it, so `mirobody/tests/test_skills.py` re-runs
all of them against the shipped resolver and the shipped Compose file on every
change. A skill that quotes a code no test derives fails the build.

The directory sits at the repository root, outside the Python package, so
`pip install mirobody` stays two packages.
