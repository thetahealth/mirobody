# Genetics on the 1.5.2 branch

> In progress. The packaged site catalog is a **one-site dbSNP sample**, so
> this branch cannot yet be released as complete genotype standardization.
> See [the roadmap](roadmap.md) for the measured release gates.

A genotype has no measurement time, unit or trend. Raw genotype uploads use a
separate collection and two tools, while genetic test **reports** still use the
observation pipeline. Neither an absent site nor a no-call is a normal result.

## Upload and storage

The genetic handler recognizes column headers, not a vendor banner. It accepts
WeGene/23andMe one-genotype columns, Ancestry two-allele columns,
MyHeritage/FTDNA CSV, several other public `snps` fixture shapes, VCF, gzip
and single-member ZIP. Multi-sample VCF requires an explicit sample. An
Illumina TOP-strand call remains `unresolved` without its manifest. Files
that only mention rsIDs in prose stay in the document pipeline.

The parser maps Ancestry 23/24/25/26 to X/Y/PAR/MT and FTDNA 0/XY to PAR.
`--`, `00` and missing VCF alleles become `no_call`. The raw spelling is
retained. A row is normalized against a versioned, read-only dbSNP index:
chromosome and coordinate must match a known build; REF/ALT and strand must
support the call before a VCF GT is assigned. Ambiguous or conflicting calls
are `unresolved`. The import records the declared build separately from the
build inferred from matching positions; `unknown` is a valid result. X/Y sex
inference needs at least 1,000 X calls; female Y no-calls become
`not_applicable` only after that threshold.

An upload writes to a `loading` genotype set. Only after all batches succeed
and the stored row count matches the parsed count does one transaction
supersede the previous active set. If a batch fails, the old set remains
queryable. The database enforces one active set per person and one row per
rsID per set. Re-uploading replaces the active collection rather than adding
a second copy. Upload status and counts appear on the Data page in the
`feat/genomics-upload` frontend branch.

### Upgrading from 1.5.1

The old `th_series_data_genetic` rows are not read by the new tools. On an
upgraded deployment run `mirobody migrate-genotypes` after applying
`32_genomics.sql` (or after starting with `BOOTSTRAP_SCHEMA=true`). The command
selects each person's latest legacy file source, keeps its most recent row for
each rsID and creates an active set only if none exists. It is safe to rerun.
`--user` limits it to one person; `--max-users` bounds one invocation. The old
rows remain for rollback or audit. The migrated calls retain their raw values
but have `build_detected=unknown`, no GT and `call_status=unresolved` except
explicit no-calls. Ask the person to upload the original export again to get
normalization; the migration never guesses a reference allele or strand.

## The packaged site index

`mirobody/res/genomics/genotype_sites.sqlite3` currently contains one public
dbSNP b157 example site, `rs268`, and three merged IDs. Its version is
`dbsnp-b157-sample` and its size is 36,864 bytes. The
[NOTICE](../mirobody/res/genomics/genotype_sites.NOTICE) records source URLs
and hashes; [the builder](../benchmarks/genomics/README.md) is reproducible.
The file is included in wheel and sdist, but does **not** establish coverage
of a consumer array. The planned 3–5 million-site union of licensed WeGene,
23andMe v5, Ancestry v2 and GSA manifests is absent. Therefore the <1%
unmatched-site target, indel definition rate, build lift-over accuracy and
whole-genome size/performance targets remain unmeasured. Ordinary chip rows
without a catalog entry are visible as raw `unresolved` rows, never as GT.

## Reading and exporting

`query_genetic_data` accepts no selector for an active-set overview, or one
of rsIDs (up to 50), HGNC gene or a bounded GRCh37/GRCh38 region. It returns
at most 500 direct rows with source, build, call status and truncation notes.
The Agent and authenticated MCP surface use the same service. Gene and region
searches require mapped sites; an unmapped raw row can still be found by rsID.
Care-circle reads require authorization. The tool does not infer disease risk.

`GET /api/v1/genomics/active-set` gives the Data page counts and provenance.
`GET /api/v1/genomics/export.vcf?build=GRCh38` (or `GRCh37`) streams only
mapped, defensible calls from the active set. Its header states how many rows
were omitted because they were unresolved, unmapped or unsuitable for export.
The VCF path has passed a two-site public-truth integration test; acceptance
by PharmCAT 3.4 on a full consumer array is still open. FHIR Genomics Variant
output is still open.

## Pharmacogenomics

`query_pharmacogenomics` matches exact CPIC generic drug names or HGNC genes;
with no selector it examines active medication plans. It reads pinned CPIC
v1.60.0 A/B drug-gene relationships and counts the defining positions called,
missing or no-call in the active upload. A guideline's evidence level is not
the person's phenotype. The tool returns `not_determined` unless validated
allele definitions, phase and required sites establish a result; it does not
issue a prescription recommendation. There is currently no validated star
allele caller, GeT-RM agreement test, CPIC version switch or recomputation.
The bundled [CPIC NOTICE](../mirobody/res/genomics/cpic-v1.60.0.NOTICE)
records the pinned source, extract and license.

Rare pathogenic array calls are not converted into disease conclusions.
Missing sites, no-calls and conflicts must not be represented as normal.

## Public-data verification and privacy

The [public-data generator](../benchmarks/genomics/generate_public_formats.py)
pins a 1000 Genomes HG00096 truth file and PharmCAT 3.4 positions by SHA-256,
then renders four upload formats from the same two calls. The
[end-to-end check](../benchmarks/genomics/e2e_public_truth.py) exercises real
WebSocket upload, active-set replacement, rsID/gene/region MCP queries, CPIC
coverage, GRCh38 VCF export and real Agent tool use. It is a pipeline check,
not evidence of whole-chip accuracy. A second check migrates only those public
truth rows from the 1.5.1 table. No owner's genotype export or personal health
document belongs in a committed fixture.

The system stores genetic data per person. Tool replies cap genotype rows;
the Data page receives summary counts only. A model may see the bounded tool
result when answering a question, so a hosted deployment must account for
its model provider and applicable consent requirements.
