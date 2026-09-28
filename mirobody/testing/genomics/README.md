# Public genotype examples

These small files ship in the wheel and sdist so an external developer can
exercise genotype parsing without a private export or a multi-gigabyte VCF.
`canonical.json` gives the expected normalized calls for 13 sites across 12
genes. The inputs are public 1000 Genomes HG00096 calls, plus a separate
HG00097 X example and one real no-call from an openly shared Harvard PGP
export. Source URLs and SHA-256 hashes are in `manifest.json` and
[SOURCES.NOTICE](SOURCES.NOTICE).
The whole PGP file and original 1000 Genomes region VCFs are **not** shipped.

| Input | What it exercises |
| --- | --- |
| `hg00096-wegene.txt`, `hg00096-23andme.txt` | One genotype column, GRCh37 |
| `hg00096-ancestry.txt` | Two allele columns, GRCh37 |
| `hg00096-myheritage.csv`, `hg00096-ftdna.csv` | Two public CSV layouts |
| `hg00096-grch37.vcf`, `hg00096-grch38.vcf` | VCF GT and two reference builds |
| `hg00096-grch37.vcf.gz`, `.vcf.bgz`, `-vcf-sidecars.zip` | gzip, block gzip, and one VCF with bounded BED/TXT sidecars |
| `pgp4220-nocall-23andme.txt` | A measured site with no result; never a reference call |
| `hg00097-x-grch37.vcf` | A separate female X call, not mixed into HG00096 |

The common internal row is **dbSNP rsID, chromosome, GRCh37/GRCh38 position,
VCF REF/ALT/GT, call status, zygosity, and an HGNC gene symbol when the
candidate catalog has one**. A VCF GT may keep phase (`1|0`); a chip export's
allele pair is unphased (`0/1`). Those are equivalent alleles but different
evidence. `raw` and `unresolved` remain explicit when the catalog cannot
verify a position or strand. This is a variant representation built from
[VCF](https://github.com/samtools/hts-specs),
[dbSNP](https://www.ncbi.nlm.nih.gov/snp/), and
[HGNC](https://hgnc.genenames.org/), not a single code system like ICPC-3 or
LOINC. The app also exports selected mapped calls as FHIR Genomics Variant
Observations using LOINC fields; that bounded export has not passed a full
profile validator.

To find an example from an installed package:

```python
from importlib.resources import files

root = files("mirobody.testing").joinpath("genomics")
print(root.joinpath("manifest.json").read_text(encoding="utf-8"))
```

With a source clone and `[app,test]` installed, run
`python -m unittest benchmarks.genomics.test_packaged_examples`. The test
checks every file hash and compares the normalized calls from all vendor and
archive renderings with `canonical.json`. To rebuild the examples, use
`benchmarks/genomics/build_packaged_examples.py` with the pinned public region
extracts and public PGP source held outside the repository.

These are format and normalization fixtures. They do not establish
whole-chip coverage, a drug phenotype, or disease risk.
