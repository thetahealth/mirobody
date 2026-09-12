# Don't Guess My Labs

**An agent skill that stops your AI from making things up about your blood test.**

Give a lab report to any coding agent and it will read it fluently and
confidently — including the rows it got wrong. It will tell you about your
neutrophil count when the page printed a neutrophil *percentage*. It will quote
a reference range from memory instead of the one printed beside the value. Hand
it a category heading like `血脂` and it will explain what your "lipids result"
means, as if that were a number.

This skill gives the agent a fact anchor: an **offline LOINC resolver that
returns nothing when it does not know a term.**

```bash
mirobody resolve "LDL cholesterol" "血红蛋白" "ヘモグロビン" "空腹血糖(GLU)" "血脂"
```

```
  LDL cholesterol  LOINC 13457-7   Cholesterol in LDL [Mass/volume] ...   [63 candidates]
  血红蛋白          LOINC 718-7     Hemoglobin [Mass/volume] in Blood      [72 candidates]
  ヘモグロビン      LOINC 718-7     Hemoglobin [Mass/volume] in Blood      [72 candidates]
  空腹血糖(GLU)     LOINC 1558-6    Fasting glucose [Mass/volume] ...
  血脂             unresolved — not in the lexical index
```

**The last line is the product.** `血脂` is a category, not an observation, so
it returns nothing instead of a plausible wrong code.

## Install

```bash
pip install mirobody
```

Two packages, ~52 MB, numpy only. **No API key. No network. No Docker.** The
vocabulary ships inside the wheel.

Then drop the skill where your agent looks for skills:

| Agent | Location |
|---|---|
| **Claude Code** | `~/.claude/skills/dont-guess-my-labs/` (global) or `.claude/skills/` (project) |
| **Codex** | your configured skills directory |
| **OpenClaw** | `~/.openclaw/skills/` or via the CLI installer |

```bash
git clone --depth 1 https://github.com/thetahealth/mirobody.git /tmp/mb
cp -r /tmp/mb/skills/dont-guess-my-labs ~/.claude/skills/
```

No restart needed.

## What it makes the agent do

1. **Read the original document**, not a summary — layout carries the panels,
   the `H`/`L` flags and the printed ranges.
2. **Keep every name verbatim** — `Total Cholesterol-TC` stays as printed.
3. **Resolve every name before explaining it.** The canonical name that comes
   back, not the model's memory, is what the row measures.
4. **Pass the value and unit too** — `中性粒细胞 62 %` is `26511-6`, while
   `中性粒细胞 4.2 10*9/L` is `26499-4`. Same name, two tests.
5. **Report unknowns as unknown**, and treat a suspiciously specific match as
   unknown too.
6. **Flag against the printed reference range**, never a remembered one.
7. **State facts, not diagnoses**, and point at what to raise with a clinician.

See [`SKILL.md`](SKILL.md) for the full workflow and
[`reference.md`](reference.md) for the confusion table — including four
**verified defects** where a category word still resolves to a specific code.

## Coverage, honestly

**211/211** on the panels an ordinary checkup prints, in English, Chinese
(Simplified and Traditional) and Japanese. 49,253 multilingual aliases over a
concept graph of 440,961 nodes.

Beyond those panels, expect gaps. `reference.md` §4 lists real terms that
resolve *wrongly* today — `维生素` ("vitamins") currently lands on a
liver-cancer risk score. The skill teaches the agent to catch that shape rather
than pretending it does not happen.

Found a term it gets wrong?
[Report it](https://github.com/thetahealth/mirobody/issues/new?template=wrong-term.yml) —
it is the highest-leverage contribution this project takes.

## Where this comes from

The resolver is one part of **[Mirobody](https://github.com/thetahealth/mirobody)**,
an Apache-2.0 health data engine: document extraction, UCUM unit normalization,
wearable ingestion, FHIR-ready records, and an agent that reads your original
files and cites the page it read from — all self-hostable with one
`./deploy.sh`, and served over MCP to Claude Desktop, Cursor or your own loop.

This skill is the 30-second version. The rest is there when you want it.

---

Apache-2.0 · [thetahealth/mirobody](https://github.com/thetahealth/mirobody)
