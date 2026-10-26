# Device field mapping

Device vendors use different names and definitions for similar measurements.
Mirobody's crosswalk maps vendor fields to internal metrics and, where a
verified match exists, LOINC codes.

## Mapping status

| Status | Meaning |
| --- | --- |
| `confident` | The vendor definition matches the reviewed LOINC axes. |
| `unverified` | A possible match needs confirmation. |
| No code | The field has no suitable mapping; the explanation is retained. |

A matching label alone is insufficient. For example, SDNN and RMSSD describe
different heart-rate-variability statistics and must not share a code simply
because both are called HRV. Proprietary scores may have no standard equivalent.

## Python lookup

The crosswalk ships with the base Python package and needs no API key:

```python
from mirobody.standardize import devices

field = devices.lookup("apple", "HKQuantityTypeIdentifier", "stepCount")
print(field.metric, field.loinc, field.confidence)
```

Mappings describe supported vocabulary, not which data a particular person's
device records. They also do not imply a ready-to-use cloud connection for
every vendor. See [device connections](https://docs.mirobody.ai/en/providers/using-providers)
for the integrations available in the application.

For the complete tables and mapping rationale, see the
[crosswalk data](../../mirobody/res/crosswalks/) and
[technical reference](../device-crosswalk.md).
