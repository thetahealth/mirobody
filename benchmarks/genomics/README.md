# Public genotype site catalog build

`build_site_catalog.py` writes `mirobody/res/genomics/genotype_sites.sqlite3`.
It reads a JSON manifest, verifies every input's SHA-256 before parsing, and
replaces the output only after a complete build. The build uses SQLite for
marker membership, merge history, selected placements and final rows; a fixed
16 MiB prefilter avoids a disk lookup for nearly every unselected dbSNP row.
Input VCF/JSON files are streamed from disk and are never loaded in full.
For a b157 bulk build, the builder checks both manifest SHA-256 and NCBI's
published `.md5` checksum for each of the three pinned bulk files. The
distributable candidate uses bounded public regions of UCSC's dbSNP 155
Common tracks; the manifest pins SHA-256 for both assembly extracts.

Run the checked-in public NCBI example without a download:

```bash
python3 benchmarks/genomics/build_site_catalog.py \
  --manifest benchmarks/genomics/sample-b157.json \
  --output /tmp/mirobody-b157-sample.sqlite3
python3 -m unittest discover -s benchmarks/genomics -p 'test_*.py'
```

The fixture is NCBI's
[`refsnp-sample.json.bz2`](https://ftp.ncbi.nlm.nih.gov/snp/archive/b157/JSON/refsnp-sample.json.bz2)
for rs268. `fixtures/markers.tsv` contains only its `refsnp_id`. It is a parser
and schema example, **not a consumer-array marker list**.

The revised G0 candidate is selected by rsIDs from two openly shared Harvard
PGP participant exports (one 23andMe v5 and one AncestryDNA v2) plus the
bundled public CPIC v1.60.0 definition sites. This is an
**observed marker union**, not either manufacturer's complete manifest. The
raw participant exports stay outside the source tree; only rsID-only lists
are generated in the ignored build directory. PGP's [open consent](https://pgp.med.harvard.edu/about)
allows reuse; the packaged SQLite contains no participant genotype, name or
file. `prepare_public_candidate.py` checks the raw file hashes and writes the
marker lists. `fetch_public_pgx_windows.py` extracts twelve bounded public
pharmacogene regions from the two UCSC dbSNP 155 Common tracks without a
whole-genome download. The bounds in `public-pgx-windows.json` derive from
public 1000 Genomes GRCh37 and PharmCAT 3.4 GRCh38 positions:

| Assembly | Source |
| --- | --- |
| GRCh37/hg19 | https://hgdownload.soe.ucsc.edu/gbdb/hg19/snp/dbSnp155Common.bb |
| GRCh38/hg38 | https://hgdownload.soe.ucsc.edu/gbdb/hg38/snp/dbSnp155Common.bb |

Install UCSC's [free `bigBedToBed` utility](https://genome.ucsc.edu/license/),
place the [public PGP 4220](https://my.pgp-hms.org/user_file/download/4220)
and [public PGP 4200](https://my.pgp-hms.org/user_file/download/4200) files
outside the repository as `23andme_v5_2023-10_male.txt` and
`ancestrydna_v2_2025-11_male.txt`, then run:

```bash
python3 benchmarks/genomics/fetch_public_pgx_windows.py \
  --output-dir internal/genomics/corpus/reference/ucsc155/pgx
python3 benchmarks/genomics/prepare_public_candidate.py \
  --pgp-dir /absolute/path/to/public-pgp \
  --ucsc-dir internal/genomics/corpus/reference/ucsc155/pgx \
  --output-dir internal/genomics/corpus/candidate
python3 benchmarks/genomics/build_site_catalog.py \
  --manifest internal/genomics/corpus/candidate/public-candidate.json \
  --output mirobody/res/genomics/genotype_sites.sqlite3
```

The fetcher checks the exact SHA-256 of both region extracts. The builder
checks input hashes and requires matching GRCh37 and GRCh38 chromosome and
REF/ALT. It excludes conflicting, non-SNV and absent sites; it does not infer
an allele. The packaged index has **489 dual-build SNV sites, 86,016 bytes**.
Of the two observed PGP marker sets it covers **262/625,705** (23andMe v5)
and **340/677,436** (Ancestry v2); it covers **41/992** CPIC definition rsIDs.
Some covered IDs occur in more than one source. The union is 489/1,137,144
distinct input rsIDs. These ratios are expected for a twelve-region preview
and are **not whole-chip coverage**. Gene symbols are present only where the
CPIC sequence locations link an rsID to a gene (41 sites, twelve distinct
gene labels); gene queries are incomplete. The index has no b157 merge
history, no I/D definitions and no WeGene/GSA manifest. An old merged rsID or
a site outside the bounded regions stays `unresolved`, even if dbSNP knows it.
`metadata.stats_json` records the exact per-source covered/total counts.

The previous b157 full-file path remains available for a future broader
candidate from redistributable marker lists. It takes these pinned sources:

| Entry | NCBI b157 URL | Archive size, bytes (HEAD, 2026-09-26) |
| --- | --- | ---: |
| `vcf37` | https://ftp.ncbi.nlm.nih.gov/snp/archive/b157/VCF/GCF_000001405.25.gz | 28,194,800,139 |
| `vcf38` | https://ftp.ncbi.nlm.nih.gov/snp/archive/b157/VCF/GCF_000001405.40.gz | 29,552,227,779 |
| `merged` | https://ftp.ncbi.nlm.nih.gov/snp/archive/b157/JSON/refsnp-merged.json.bz2 | 813,797,312 |

The three NCBI b157 files total **58,560,825,230 bytes** compressed. They
have not been downloaded here. The revised candidate makes no claim about a
3–5 million-site manufacturer union, WeGene/GSA coverage or the former <1%
unmatched-site target. The one-site example is a parser fixture only.

Packaging includes `**/*.sqlite3`; `scripts/check_wheel_data.py` checks both
wheel and sdist for the index, its NOTICE and the CPIC extract.

`generate_public_formats.py` uses pinned public 1000 Genomes and PharmCAT
inputs to render one truth sample in 23andMe, Ancestry, MyHeritage and VCF
shapes, plus gzip/zip and GRCh38 VCF renderings. `e2e_public_truth.py` runs
nine upload → active set → MCP → VCF/FHIR paths and
optionally two real Agent questions. `e2e_legacy_migration.py` checks that
1.5.1 rows migrate conservatively. Generated raw truth stays outside the
source tree; the local `internal/genomics/corpus/` link points to that
private working directory and is not in the distribution.
Pass `--agent --guard-log /path/to/isolated-server.json.log` to make both
questions share a checkpoint session and assert that every model boundary
has `visible <= returned` genotype rows, with the previous answer redacted.
The isolated live run observed three such boundaries.
The 40-question public-truth Agent check selected the expected tool in
37/40 Qwen first turns, with zero automated forbidden-claim alarms. Its
generic "no question" replies in questions 12, 25 and 37 count as invalid
answers; two Qwen and one OpenRouter GPT targeted reruns recovered them.
Keep the answer JSONL outside the repository and review it for claims the
lexical alarm cannot detect.
`eval_agent_public.py --audit-existing --output /path/to/answers.jsonl` re-scores
all 40 saved answers without a model key and enforces the preview floor:
at least 36 correct tool selections, at least 36 valid first answers and
zero automated forbidden-claim matches. The three generic Qwen replies
remain recorded as misses even though targeted reruns recovered them.

`e2e_public_candidate.py --pgp-dir /absolute/path/to/public-pgp` also uploads
the two complete open PGP exports through the real WebSocket path to isolated
PostgreSQL. The 23andMe v5 export produced 643,535 stored rows and 258 called
rows; the Ancestry v2 export replaced it with 677,436 rows and 339 called.
Both active sets and an rsID MCP lookup passed. These low called counts make
the regional coverage limit visible in an actual import, not just the marker
set intersection. Raw participant files remain outside the source tree.
The Ancestry v2 public export stored 25,242 X, 1,665 Y, 36 PAR and 263 MT
rows (27,206 total). A bounded `chromosome=PAR, build=raw` MCP lookup returned
the pinned public rs28736870 row; `raw` is labelled as an upload coordinate,
not a verified reference assembly.
Pass `--include-bigy` with the full PGP corpus to upload its public ZIP with a
VCF and BED/TXT sidecars as a third replacement. That path activated 444,297
rows, all with GTs described by the VCF itself. The raw two alternate contig
names and four long ALT strings require the widened text columns in
`32_genomics.sql`; they are not evidence of catalogue-verified calls.
The parser's separate public-corpus check classified all 17 accessible PGP
genotype exports, including two BGZF WGS VCFs with 30–146 KiB headers and a
Big-Y ZIP containing one VCF plus BED/TXT sidecars. The sole HTML report was
not classified as genotype. All 17 original public genotype exports were also
uploaded through the isolated WebSocket/PostgreSQL path, using
`e2e_public_pgp_corpus.py` for 14 IDs and separate pinned checks for Big-Y
3779 and WGS 4176/4182. Each produced an active set and MCP overview. The
two BGZF WGS files produced 4,741,304/4,741,304 and
5,017,551/5,017,547 stored/called rows; the uncompressed VCF 1241 produced
2,294,794/2,282,175. These VCF calls use their own REF/ALT and do not
measure the 489-site candidate's whole-genome coverage. The PGP 179 NCBI36
export produced zero normalized calls, an expected explicit coverage limit.
Raw participant files stay outside the source tree.
Set `MIROBODY_PUBLIC_PGP_DIR` to an external directory containing the pinned
public PGP `MANIFEST.json` and its downloads, then run
`python3 -m unittest benchmarks.genomics.test_public_pgp_classification` to
repeat all 18 hash and classification checks. A clone without that corpus
skips this optional gate.
Use `e2e_public_pgp_corpus.py --pgp-dir ... --base ...` to repeat the full
original-file uploads; `--ids` selects public PGP IDs for a resumed run.

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
After adding the explicit raw-coordinate index for PAR/other unmapped rows,
the same 1.3-million-row public GIAB import in the isolated audit schema took
42.510 s and added 280,338,432 bytes of table plus indexes. It remains below
the 60 s write gate; the extra raw index accounts for additional storage.
Pass `--schema audit_genomics_152` (or another disposable schema) when the
test database also hosts an application schema.

The final schema is `metadata(key,value)`,
`sites(rsid,chrom,pos37,pos38,ref,alt,gene)`, and
`merged(old_rsid,rsid)`. `sites.rsid` and `merged.old_rsid` are primary keys;
`sites(chrom,pos37)` and `sites(chrom,pos38)` are indexed. `ref` and comma-
separated `alt` describe GRCh38, and a site is omitted if its REF/ALT differs
between the two builds because this schema cannot represent both safely.
`gene` comes from dbSNP's annotation and is empty when it is absent. The
`metadata` table records `version`, `scope`, source URL/hash/licence JSON and
counts. `scope=candidate` describes the pinned public input set; it does not
assert manufacturer coverage, size or I/D gates.
