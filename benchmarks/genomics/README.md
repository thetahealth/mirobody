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
shapes. `e2e_public_truth.py` runs the upload → active set → MCP → VCF path and
optionally two real Agent questions. `e2e_legacy_migration.py` checks that
1.5.1 rows migrate conservatively. Generated raw truth stays under ignored
`internal/genomics/corpus/`, not in the distribution.

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
