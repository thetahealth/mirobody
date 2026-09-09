# The whole engine, in four minutes

The care-circle walkthrough the README used to carry in full. Each part is
recorded against a running `./deploy.sh` stack with `SEED_DEMO_DATA` on; the
README shows three of the four scenes and links here for the fourth.


`SEED_DEMO_DATA` defaults to on, so the ① → ② → ③ chain is walkable the moment
`./deploy.sh` finishes — signing in and browsing the seeded record need no
key; the upload extraction in part 2 and the questions after it ride the one
key configured above. Four parts, each recorded against the running stack.

**1 · Arrive.** You sign in owning a **thin** record — a few weeks of
self-tracked vitals and one unremarkable checkup, seeded as your own — and find
one synthetic person sharing a **thick** one with you: **Demo (synthetic)**,
244 indicators and 14,273 readings across two years, five documents the agent
can `read_file`. Same question, two records: *your* HbA1c answers with one
boring-normal value from data you own; *hers* answers with a two-year story
from data you can only view. Isolation you can see, not just read about.

<p align="center">
  <img src="images/care-circle-demo.gif"
       alt="Your own account's indicators and an uploaded report, then switching to Demo's shared record and opening two years of HbA1c" width="880">
</p>

<div align="center">
<img src="images/your-care-circle.svg" alt="Your own thin record next to hers — the thick one you can only view." width="820">
</div>

The switch in that diagram is a column, not a promise:
`care_circle_members.health_access`, `NOT NULL DEFAULT 0`, on **your own** row.
Being invited into a circle shares nothing — the member decides, and no other
person's action can raise it. The check that reads it raises rather than
returning a falsy value, so a route that forgets to look answers 403 instead of
handing over a record.
[`examples/06_care_circle_rules.py`](../examples/06_care_circle_rules.py) prints
the whole decision table offline.

**2 · ③ Answer (agent), on someone else's record.** Ask about her HbA1c and the agent
finds the data itself, cross-references the lab draws against the sensor-derived
series, and charts both — then tells you the improvement did not hold.

<p align="center">
  <img src="images/ask-circle-demo.gif"
       alt="Asking about the shared record's HbA1c; the agent queries, charts lab and sensor series together, and reads the trend" width="880">
</p>

```
lab-drawn HbA1c   7.2 % (2024-04)  →  6.5 % (2024-10)  →  6.6 % (2025-04)
                  only 3 lab draws in two years — the sensor eA1C has 104
```

**3 · ① Collect + ② Translate (standardize), on your own.**
`demo/lab_report_2025-10-15.pdf` is a panel deliberately held out of the
seed, so uploading it is not a no-op. Drop it on the Data page and twelve
analytes come out with their values and units in seconds, each linking back to
the page it was read from.

<p align="center">
  <img src="images/upload-demo.gif"
       alt="Dropping a lab-report PDF on the Data page; twelve analytes extracted, each linked to its source file" width="880">
</p>

**4 · ③ Answer (agent), on what you just uploaded.** Ask again, now about your own
record. The agent reads the report through the virtual filesystem, flags all
twelve results against their printed reference ranges — and says plainly that one
date is not a trend.

<p align="center">
  <img src="images/ask-own-demo.gif"
       alt="Asking about your own just-uploaded panel; the agent reads the report and flags every result against its reference range" width="880">
</p>

That contrast is the demo's point: **two years of history buys a trend, one panel
buys an interpretation.** Both answers cite what they read.

Every value is synthetic — generated for ESL-Bench by
[mirobody-eval](https://github.com/thetahealth/mirobody-eval) and vendored, so the
seed needs no network and no key. Set `SEED_DEMO_DATA=false` for a deployment that
will hold real data. What the extraction pass does *not* yet do with those twelve
readings is written down in [docs/roadmap.md](roadmap.md) rather than glossed
over here.

---

