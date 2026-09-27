# Public dbSNP b157 site catalog build

`build_site_catalog.py` writes `mirobody/res/genomics/genotype_sites.sqlite3`.
It reads a JSON manifest, verifies every input's SHA-256 before parsing, and
replaces the output only after a complete build. The build uses SQLite for
marker membership, merge history, selected placements and final rows; a fixed
16 MiB prefilter avoids a disk lookup for nearly every unselected dbSNP row.
Input VCF/JSON files are streamed from disk and are never loaded in full.
For a full candidate, the builder checks both manifest SHA-256 and NCBI's
published `.md5` checksum for each of the three pinned bulk files.

Run the checked-in public NCBI example without a download:

```bash
python3 benchmarks/genomics/build_site_catalog.py \
  --manifest benchmarks/genomics/sample-b157.json \
  --output mirobody/res/genomics/genotype_sites.sqlite3
python3 -m unittest discover -s benchmarks/genomics -p 'test_*.py'
```

The fixture is NCBI's
[`refsnp-sample.json.bz2`](https://ftp.ncbi.nlm.nih.gov/snp/archive/b157/JSON/refsnp-sample.json.bz2)
for rs268. `fixtures/markers.tsv` contains only its `refsnp_id`. It is a parser
and schema example, **not a consumer-array marker list**.

For a full candidate build, supply a separate manifest with exactly four
marker lists named `wegene`, `23andme_v5`, `ancestry_v2`, `gsa`; each must be a
UTF-8 file headed `rsid`, followed by one rsID per line. Each manifest entry
must give `path`, public `url`, SPDX-style `license` from the builder's allow
list, and the downloaded file's `sha256`. The two VCF entries must be named
`vcf37` and `vcf38`, and `merged` must name the b157 merged RefSNP JSONL file.
Use the manifest shape in `sample-b157.json`. The full-build URLs are pinned:

| Entry | NCBI b157 URL | Archive size, bytes (HEAD, 2026-09-26) |
| --- | --- | ---: |
| `vcf37` | https://ftp.ncbi.nlm.nih.gov/snp/archive/b157/VCF/GCF_000001405.25.gz | 28,194,800,139 |
| `vcf38` | https://ftp.ncbi.nlm.nih.gov/snp/archive/b157/VCF/GCF_000001405.40.gz | 29,552,227,779 |
| `merged` | https://ftp.ncbi.nlm.nih.gov/snp/archive/b157/JSON/refsnp-merged.json.bz2 | 813,797,312 |

The three NCBI files total **58,560,825,230 bytes** compressed, before the
four marker lists or scratch SQLite file. They have not been downloaded here.
Four complete, publicly licensed marker lists have not been supplied in this
workspace. Consequently the checked-in 1-row sample is **not G0 acceptance**:
coverage of any WeGene/23andMe v5/Ancestry v2/GSA export, the estimated 3–5
million-row union, and the plan's <1% unmatched-site gate remain unmeasured.
No personal genotype export is an input to this build.

Packaging includes `**/*.sqlite3`; `scripts/check_wheel_data.py` checks both
wheel and sdist for the sample index, its NOTICE and the CPIC extract. This
packages the lookup mechanism, but it does not satisfy the full G0 site
coverage gate.

`generate_public_formats.py` uses pinned public 1000 Genomes and PharmCAT
inputs to render one truth sample in 23andMe, Ancestry, MyHeritage and VCF
shapes, plus gzip/zip and GRCh38 VCF renderings. `e2e_public_truth.py` runs
seven upload → active set → MCP → VCF/FHIR paths and
optionally two real Agent questions. `e2e_legacy_migration.py` checks that
1.5.1 rows migrate conservatively. Generated raw truth stays under ignored
`internal/genomics/corpus/`, not in the distribution.

`fixtures/public-hg00096.vcf` is the two-call VCF rendering of 1000 Genomes
phase 3 public male sample HG00096 from the pinned GRCh37 CYP2C19 region VCF
(`generate_public_formats.py` verifies SHA-256
`c63f2e17f9fa7ed06d75c0c03824233eced60910d2a2e5a04cb7b51cab0921ca`).
The regression test adds comment padding only; it does not invent calls.
GRCh37 PAR bounds follow the GRC human assembly report and GRCh38 PAR bounds
follow Ensembl's human PAR annotation.

`fixtures/public-1000g-x.tsv` contains four unchanged phased GTs from the
[official 1000 Genomes phase 3 GRCh37 X VCF](https://ftp.1000genomes.ebi.ac.uk/vol1/ftp/release/20130502/ALL.chrX.phase3_shapeit2_mvncall_integrated_v1c.20130502.genotypes.vcf.gz):
HG00096 is male and HG00097 is female in the project's pinned sample panel.
The female non-PAR heterozygote is deliberately evaluated under inferred male
sex to exercise a conflicting upload; its alleles are copied from public truth.

`test_cpic_fetch.py` uses the official CPIC v1.59.1 SQL dump in the ignored
`internal/genomics/corpus/reference/` directory. It checks exact-version
fetch, local version selection and rollback on a truncated download. The
public source SHA-256 is pinned in the test; a clone without the dump skips
this integration case. CPIC v1.60.0 is the bundled version. `mirobody fetch
cpic --version latest` consults CPIC's official release metadata and skips
application-only releases that published no database image.
With an isolated PostgreSQL schema, `check_ploidy_activation.py` checks the
transaction's status and `n_called`; `check_filesystem_privacy.py` checks
plain/gzip/zip chat scene persistence and both Agent file mounts. Set
`MIROBODY_GENOMICS_TEST_DSN` and pass `--schema public` for a public-schema
test database, or use the configured `PG_SCHEMA`.

`benchmark_public_bulk.py` selects the first 1.3 million unique called
biallelic dbSNP SNVs from the pinned public NIST GIAB HG005 GRCh37 benchmark
VCF. The generated 54,300,855-byte VCF stays under ignored `internal/genomics`.
Against isolated PostgreSQL, the original 50,000-row executemany batches took
80.254 s and added 239,345,664 bytes of table plus indexes; column-array
batches took 42.074 s and added 239,681,536 bytes. This measures storage,
not consumer-array normalization or build lift.

The final schema is `metadata(key,value)`,
`sites(rsid,chrom,pos37,pos38,ref,alt,gene)`, and
`merged(old_rsid,rsid)`. `sites.rsid` and `merged.old_rsid` are primary keys;
`sites(chrom,pos37)` and `sites(chrom,pos38)` are indexed. `ref` and comma-
separated `alt` describe GRCh38, and a site is omitted if its REF/ALT differs
between the two builds because this schema cannot represent both safely.
`gene` comes from dbSNP's annotation and is empty when it is absent. The
`metadata` table records `version`, `scope`, source URL/hash/licence JSON and
counts. `scope=candidate` only means all required *input classes* were given;
it does not assert G0's coverage, size or I/D gates.
