# Small health-record vocabulary benchmark

`cases.json` contains 13 synthetic indicator readings and 26 short example
complaint phrases in English, Simplified and Traditional Chinese, Japanese and
Russian, the languages the resolver covers (Russian lab-panel terms arrived
with #88; Estonian is lab-only and left out). Each language has a case on both
vocabularies, and the test fails if one is dropped. The fixed results show
LOINC coding and UCUM units for readings, and the separate ICPC-3 symptom and
diagnosis axes for complaints.
Over-broad, ambiguous and unmatched phrases retain local identities instead
of acquiring guessed codes. The person's original phrase remains the display
text of a reading; an application recording a complaint must retain that text
beside the code.

Run from a source clone with `pip install -e '.[app,test]'`:

```bash
python -m unittest benchmarks.health_records.test_cases
```

These are offline vocabulary and FHIR Observation cases, not a database or Agent benchmark. Values
are synthetic. Chinese, Japanese and Russian complaint phrases are Mirobody's
own curated surfaces, not purported official ICPC-3 translations; Traditional
Chinese reaches them through the same zh-Hant fold the LOINC side uses, and a
Taiwan word that differs (氣喘, asthma) is curated under its own spelling. See
[`mirobody/res/icpc3/icpc3.NOTICE`](../../mirobody/res/icpc3/icpc3.NOTICE).
