# Small health-record vocabulary benchmark

`cases.json` contains five synthetic indicator readings and nine short example
complaint phrases. The fixed results show LOINC coding and UCUM units for
readings, and the separate ICPC-3 symptom and diagnosis axes for complaints.
Over-broad, ambiguous and unmatched phrases retain local identities instead
of acquiring guessed codes. The person's original phrase remains the display
text of a reading; an application recording a complaint must retain that text
beside the code.

Run from a source clone with `pip install -e '.[app,test]'`:

```bash
python -m unittest benchmarks.health_records.test_cases
```

These are offline vocabulary and FHIR Observation cases, not a database or Agent benchmark. Values
are synthetic. Chinese complaint phrases are Mirobody's own curated surfaces,
not purported official Chinese ICPC-3 terms; see
[`mirobody/res/icpc3/icpc3.NOTICE`](../../mirobody/res/icpc3/icpc3.NOTICE).
