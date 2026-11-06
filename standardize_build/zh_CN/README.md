# `standardize_build/zh_CN/`: the Chinese LOINC translation, as it was reviewed

LOINC's Simplified Chinese linguistic variant (zh-CN, variant ID 5, in LOINC 2.83)
translates the six Parts of each code and leaves `LONG_COMMON_NAME`,
`LinguisticVariantDisplayName` and `ConsumerName` empty. These files are the
review of those Parts and the translation of LOINC's English consumer names;
`mirobody/res/loinc/zh_CN_names.tsv` holds the display and long names the runtime
reads.

| File | What it is |
| --- | --- |
| `parts.tsv` | Every Part used by the 17,555 codes with a `COMMON_TEST_RANK`: `axis`, the English Part, the variant's rendering (`official`, every rendering in use when the variant has more than one), the reviewed one (`chinese`) and, when they differ beyond spacing, why (`note`). `standardize_build/loinc_zh.py` reads it to build `res/loinc/zh_CN_parts.tsv.gz`. |
| `STYLE.md` | The rules every Part, name and consumer name was reviewed or written by, with the two glossaries. |
| `consumer.tsv` | LOINC's English `ConsumerName` and its Chinese translation, one row per distinct English name, so sibling codes cannot disagree. Not read at runtime; it goes into the submission file. |

How they were made. Each Part was reviewed against the English Part and corrected
where the variant was wrong, stale against the current English Part, or not the
standard Chinese term; a second, independent reader tried to refute every
proposed change; Parts sharing an English stem were then made to read alike, and
one typographic rule (a space between Chinese and Latin letters or digits, none
between Chinese characters) applied to all. Names and consumer names were drafted
from the reviewed Parts and read again by an independent reviewer, whose
corrections were checked before they were applied. Language models did the
drafting and the reading, following `STYLE.md` (the glossaries for LOINC's bracket
labels and specimens are there too); a correction should follow it as well.

Licence. These are translations of LOINC. Under section 12 of the LOINC License
(5.8) they are assigned to Regenstrief Institute and transmitted to them
(`python -m standardize_build.loinc_zh --loinc <release> --submission <file>`
writes them in the LinguisticVariant CSV layout). They are distributed under the
LOINC License, not this project's: see `mirobody/res/loinc/zh_CN.NOTICE`.
