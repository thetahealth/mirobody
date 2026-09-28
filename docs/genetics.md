# Genetics on the 1.5.2 branch

> 1.5.2 genetics preview. The packaged site catalog is a **489-site public
> candidate** bounded to twelve pharmacogene regions. The preview provides
> genotype facts and CPIC coverage; it returns `not_determined` for drug
> phenotypes. Whole-chip standardization, GeT-RM phenotype agreement and
> rare-pathogenic interpretation are deferred.
> See [the roadmap](roadmap.md) for the measured release gates.

A genotype has no measurement time, unit or trend. Raw genotype uploads use a
separate collection and two tools, while genetic test **reports** still use the
observation pipeline. Neither an absent site nor a no-call is a normal result.

## Upload and storage

The genetic handler recognizes column headers, not a vendor banner. It accepts
WeGene/23andMe one-genotype columns, Ancestry two-allele columns,
MyHeritage/FTDNA CSV, several other public `snps` fixture shapes, VCF, gzip,
BGZF and ZIP. A ZIP may hold one VCF plus bounded BED/TXT sidecars; multiple
genotype files remain ambiguous and are refused. Multi-sample VCF requires an explicit sample. An
Illumina TOP-strand call remains `unresolved` without its manifest. Files
that only mention rsIDs in prose stay in the document pipeline.
Noncanonical VCF contig names are retained as raw chromosome text; the
canonical region query and VCF export omit them rather than inventing a lift.

The parser maps Ancestry 23/24/25/26 to X/Y/PAR/MT and FTDNA 0/XY to PAR.
`--`, `00` and missing VCF alleles become `no_call`. The raw spelling is
retained. A row is normalized against a versioned, read-only dbSNP index:
chromosome and coordinate must match a known build; REF/ALT and strand must
support the call before a VCF GT is assigned. Ambiguous or conflicting calls
are `unresolved`. The import records the declared build separately from the
build inferred from matching positions; `unknown` is a valid result. X/Y sex
inference needs at least 1,000 X calls; female Y no-calls become
`not_applicable` only after that threshold. After sex inference, male non-PAR
X/Y calls with distinct alleles become `unresolved / haploid_conflict`;
unknown PAR position becomes `unresolved / par_unknown` for multi-allele GTs.
PAR calls remain diploid. The active set's `n_called` is counted after these
corrections, inside the activation transaction.

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

`mirobody/res/genomics/genotype_sites.sqlite3` contains 489 public dbSNP 155
Common SNVs with matching GRCh37/GRCh38 chromosome and REF/ALT in twelve
pharmacogene regions. Its version is `dbsnp-b155-common-pgx-candidate` and its
size is 86,016 bytes. The
[NOTICE](../mirobody/res/genomics/genotype_sites.NOTICE) records source URLs
and hashes; [the builder](../benchmarks/genomics/README.md) is reproducible.
The file is included in wheel and sdist. It covers 262 of 625,705 observed
rsIDs in one openly shared PGP 23andMe v5 export, 340 of 677,436 in one PGP
AncestryDNA v2 export, and 41 of 992 public CPIC definition rsIDs. These
observed exports are not manufacturer manifests. Under the revised G0 scope,
the index is an explicitly limited public candidate; it does **not** establish
consumer-array coverage. WeGene/GSA coverage, indel definitions, merged rsID
history, rare variants, broad gene annotation, full-chip build lift-over
accuracy and whole-chip size/performance are absent or unmeasured. Ordinary
chip rows without a catalog entry are visible as raw `unresolved` rows,
never as GT. A gene query is incomplete outside the 41 CPIC-labelled sites.
An isolated PostgreSQL upload of the two complete public PGP exports yielded
643,535 rows / 258 called for 23andMe v5, then atomically replaced that set
with 677,436 rows / 339 called for Ancestry v2. The active-set and MCP rsID
checks passed. These counts show how narrow the candidate currently is.
The public Ancestry v2 export's X/Y/PAR/MT rows total 27,206
(25,242/1,665/36/263); a bounded PAR query passed in explicit raw-coordinate
mode. This says the rows are retained and queryable, not that their PAR
coordinates have been independently lifted to both assemblies.
The public multi-member Big-Y ZIP also uploaded in full: 444,297 rows active
with 444,297 self-described VCF GT calls, including two alternate contigs and
four ALT strings longer than the early preview schema allowed. These VCF
calls reflect that file's own REF/ALT, not cross-checked catalogue coverage.
All 17 accessible original PGP genotype exports have now activated through
WebSocket and isolated PostgreSQL; the one HTML report was rejected. The two
public BGZF WGS VCFs stored 4,741,304 and 5,017,551 rows respectively.
Their high VCF `called` counts use each file's own REF/ALT and do not imply
that the candidate index verifies those whole genomes.

A separate public-data storage benchmark imported 1.3 million unique, called
biallelic SNVs from NIST GIAB HG005 into isolated PostgreSQL. The original
batched insert took 80.254 s and added 239,345,664 bytes of table and
indexes; the column-array insert took 42.074 s and added 239,681,536 bytes.
These figures do not establish vendor-array catalog coverage or GRCh38 mapping
accuracy: the packaged catalog only maps its bounded candidate sites.
With the additional raw-coordinate index, the 1.3-million-row public GIAB
import took 42.510 s and added 280,338,432 bytes in an isolated audit schema.
It remains under the 60 s storage gate; this is a different index layout from
the earlier 239,681,536-byte run.

## Reading and exporting

`query_genetic_data` accepts no selector for an active-set overview, or one
of rsIDs (up to 50), HGNC gene or a bounded GRCh37/GRCh38 region. A region may
also use `build=raw` to search upload positions without claiming an assembly;
PAR rows require this raw mode. It returns
at most 500 direct rows with source, build, call status and truncation notes.
For a region query, `position` uses the requested build; `query_build`,
`raw_position`, `pos37` and `pos38` make its provenance explicit.
The Agent and authenticated MCP surface use the same service. Gene and region
searches require mapped sites; an unmapped raw row can still be found by rsID.
Care-circle reads require authorization. The tool does not infer disease risk.

`GET /api/v1/genomics/active-set` gives the Data page counts and provenance.
`GET /api/v1/genomics/export.vcf?build=GRCh38` (or `GRCh37`) streams only
mapped, defensible calls from the active set. Its header states how many rows
were omitted because they were unresolved, unmapped or unsuitable for export,
and records the upload format, vendor, declared/detected build and normalizer
and site-catalog versions. Contigs are emitted in numeric order followed by
X, Y and MT.
The VCF path has passed a two-site public-truth integration test and PharmCAT
3.4.0 accepts that export with no VCF warnings. Its matcher returns two
candidate CYP2C19 diplotypes, so full-array named-allele agreement remains
open. `GET /api/v1/genomics/export.fhir.json?rsids=rs4244285` returns a
bounded FHIR STU3 Variant Observation collection inside the normal API
envelope. It uses the active upload, requires the same authorization as the
genotype tool, and returns `missing_rsids` and `omitted_rsids`. Only mapped
called or no-call sites with coordinates and REF/ALT can be represented; the
public two-site upload passes this export check in nine renderings. A full
profile-validator run and whole-chip coverage remain open.

## Pharmacogenomics

`query_pharmacogenomics` matches exact CPIC generic drug names or HGNC genes;
with no selector it examines active medication plans. It reads pinned CPIC
v1.60.0 A/B drug-gene relationships and counts the defining positions called,
missing or no-call in the active upload. A guideline's evidence level is not
the person's phenotype. The tool returns `not_determined` unless validated
allele definitions, phase and required sites establish a result; it does not
issue a prescription recommendation. There is currently no validated star
allele caller, GeT-RM agreement test or stored-result recomputation.
The bundled [CPIC NOTICE](../mirobody/res/genomics/cpic-v1.60.0.NOTICE)
records the pinned source, extract and license.

`mirobody fetch cpic --version v1.59.1` downloads an exact public CPIC release
into `CPIC_DIR` (default `~/.mirobody/cpic`), validates its COPY schema as data,
and installs the extracted tables atomically. `--version latest` resolves the
newest official release that published a database dump at fetch time. Set `CPIC_SOURCE_URL` to a mirror's
base URL when needed. Set `CPIC_VERSION` to an installed exact version or
`latest` to select the newest **locally installed** extract for subsequent
queries; `bundled` always uses the tested packaged v1.60.0 data. Fetching a
version does not activate it. Queries read the selected version at request
time; no PGx interpretation is persisted or recomputed yet. The pinned public
v1.59.1 and v1.60.0 dumps passed fetch, validation and version-selection tests.

Rare pathogenic array calls are not converted into disease conclusions.
Missing sites, no-calls and conflicts must not be represented as normal.

## Public-data verification and privacy

The [public-data generator](../benchmarks/genomics/generate_public_formats.py)
pins a 1000 Genomes HG00096 truth file and PharmCAT 3.4 positions by SHA-256,
then renders nine upload files from the same two calls, including gzip, BGZF,
ZIP with sidecars and
both reference assemblies. The
[end-to-end check](../benchmarks/genomics/e2e_public_truth.py) exercises the
packaged candidate index through real
WebSocket upload, active-set replacement, rsID/gene/region MCP queries, CPIC
coverage, GRCh37/38 VCF and FHIR export and real Agent tool use. The public
rs4244285 VCF lists ALT `A` while the candidate dbSNP site lists `A,C,T`;
the normalizer maps its GT index to the catalog allele order. It is a pipeline check,
not evidence of whole-chip accuracy. A second check migrates only those public
truth rows from the 1.5.1 table. No owner's genotype export or personal health
document belongs in a committed fixture.

The 40-question public-truth Agent evaluation selected the expected genetic
tool in 40/40 OpenAI runs and produced 40/40 nonempty answers, with no match
for its automated positive-claim alarm. This measures tool choice over two
sites; it does not validate a clinical interpretation or the entire model
answer. The evaluation corpus and answers remain outside the repository.

The system stores genetic data per person. Tool replies cap genotype rows;
raw genotype files are excluded from the Agent's `/uploads/` and `/library/`
document mounts, including when the file is attached to a chat message.
Older rows mislabeled as reports are screened by their genotype header or
archive type, and rows linked to genotype sets are excluded. Reads and
downloads also inspect raw bytes when the stored text is absent. The
Data page receives summary counts only. A model may see the bounded tool
result when answering a question, so a hosted deployment must account for
its model provider and applicable consent requirements. The
[live privacy check](../benchmarks/genomics/check_filesystem_privacy.py) covers
plain/gzip/zip chat classification, row persistence and both document mounts.
The model-call guard counts current-turn genetic tool rows and strips previous
turn's genetic tool results and dependent answers from checkpoint replay. An
isolated two-question live check logged three model boundaries: every visible
genotype row was accounted for by a current-turn tool result, and the prior
row and dependent answer were removed on replay. DeepAgents' separate
summarizer and history offload also receive redacted genotype results,
including during context overflow recovery; public-call tests cover the sync
and async paths. The general-purpose subagent is disabled in this harness.
After a genetic query,
the Agent refuses scratch-file reads and writes while still allowing read-only
access to the document and profile mounts. A forced live summarizer/offload
run remains unmeasured; those indirect paths have public-call regression tests.
