# Genetics

Mirobody keeps a raw genotype upload as facts: one call per site, with the
build, allele and strand checks that produced it. Two tools read those facts:
`query_genetic_data` for sites, genes and regions, and
`query_pharmacogenomics` for CPIC drug-gene coverage. The scope is bounded on
purpose:

- **Sites.** The bundled index maps 489 public dbSNP 155 Common SNVs in twelve
  pharmacogene regions to both genome builds. Every other row of an upload is
  kept as uploaded and marked `unresolved`. It can still be found by rsID or
  raw coordinate, but it never gets a genotype the index cannot support.
- **Drugs.** An answer reports which CPIC defining sites were called, missing
  or no-call, and returns `not_determined` for the phenotype. A star allele
  needs phase, copy number and every defining site, and an array export
  carries none of these reliably.
- **Disease.** A rare pathogenic call is never turned into a disease
  conclusion.

A genotype has no measurement time, unit or trend. Raw genotype uploads use a
separate collection and these two tools. Genetic test **reports** still use
the observation pipeline. Neither an absent site nor a no-call is a normal
result.

## The common representation

Different consumer exports are parsed into one site/call shape: **dbSNP rsID
when known, chromosome, GRCh37/GRCh38 coordinates when verified, REF/ALT,
VCF-style GT, call status, zygosity, optional HGNC gene symbol, and the raw
upload spelling and provenance**. The [VCF specification](https://github.com/samtools/hts-specs)
defines REF/ALT/GT; [dbSNP](https://www.ncbi.nlm.nih.gov/snp/) identifies
RefSNP sites; [HGNC](https://hgnc.genenames.org/) names human genes. A chip
allele pair becomes a GT only when the site index supports its assembly,
coordinates and strand. A VCF can carry its own REF/ALT/GT. That does not
mean the site index verified the whole file.

This serves the same interoperability purpose as ICPC-3 for reported
complaints and LOINC for measured indicators. A genotype, however, has no
single equivalent code: its identity also depends on the reference assembly
and alleles. The [FHIR Genomics Reporting STU3](https://hl7.org/fhir/uv/genomics-reporting/STU3/index.html)
export carries mapped variant facts as LOINC observation and component codes.
It is a bounded Variant Observation collection, not a claim of conformance to
every profile in that guide.

Small, publicly sourced [developer examples](../mirobody/testing/genomics/README.md)
ship with the wheel: 13 verified calls across 12 genes. They come in five
vendor-shaped text/CSV layouts and as VCF in GRCh37, GRCh38, gzip, BGZF and
ZIP, plus a separate public no-call and a female X example. `canonical.json`
is the expected common result; `manifest.json` pins every file and its
source. Original participant exports and whole-genome files stay outside the
package.

## Upload and storage

The genetic handler recognizes column headers, not a vendor banner. It accepts
WeGene/23andMe one-genotype columns, Ancestry two-allele columns,
MyHeritage/FTDNA CSV, several other public `snps` fixture shapes, VCF, gzip,
BGZF and ZIP. A ZIP may hold one VCF plus bounded BED/TXT sidecars; multiple
genotype files are ambiguous and are refused. A multi-sample VCF is refused,
because nothing in the upload names the person's sample. An Illumina
TOP-strand call remains `unresolved` without its manifest. Files that only
mention rsIDs in prose stay in the document pipeline. Noncanonical VCF contig
names are kept as raw chromosome text; the canonical region query and VCF
export omit them rather than inventing a lift.

The parser maps Ancestry 23/24/25/26 to X/Y/PAR/MT and FTDNA 0/XY to PAR.
`--`, `00` and missing VCF alleles become `no_call`. The raw spelling is
kept. A row is normalized against a versioned, read-only dbSNP index:
chromosome and coordinate must match a known build; REF/ALT and strand must
support the call before a VCF GT is assigned. Ambiguous or conflicting calls
are `unresolved`. The import records the declared build separately from the
build inferred from matching positions. `unknown` is a valid result: it is
recorded whenever fewer than 20 positions vote or no build has 90% of the
votes, so the bundled 13-site examples always report it. X/Y sex inference needs at
least 1,000 X calls; female Y no-calls become `not_applicable` only after that
threshold. After sex inference, male non-PAR X/Y calls with distinct alleles
become `unresolved / haploid_conflict`; an unknown PAR position becomes
`unresolved / par_unknown` for multi-allele GTs. PAR calls remain diploid. The
active set's `n_called` is counted after these corrections, inside the
activation transaction.

An upload writes to a `loading` genotype set. Only after all batches succeed
and the stored row count matches the parsed count does one transaction
supersede the previous active set. If a batch fails, the old set remains
queryable. The database enforces one active set per person and one row per
rsID per set. Re-uploading replaces the active collection rather than adding
a second copy. The Data page of the bundled web client shows the upload's
status and the active set's counts.

### Upgrading from 1.5.1

The old `th_series_data_genetic` rows are not read by the new tools. On an
upgraded deployment run `mirobody migrate-genotypes` after applying
`32_genomics.sql` (or after starting with `BOOTSTRAP_SCHEMA=true`). The command
selects each person's latest legacy file source, keeps its most recent row for
each rsID and creates an active set only if none exists. It is safe to rerun.
`--user` limits it to one person; `--max-users` bounds one invocation. The old
rows remain for rollback or audit. The migrated calls keep their raw values
but have `build_detected=unknown`, no GT and `call_status=unresolved` except
explicit no-calls. Ask the person to upload the original export again to get
normalization; the migration never guesses a reference allele or strand.

## The bundled site index

`mirobody/res/genomics/genotype_sites.sqlite3` holds 489 public dbSNP 155
Common SNVs whose GRCh37 and GRCh38 chromosome and REF/ALT agree, in twelve
pharmacogene regions. The file is 86,016 bytes and ships in the wheel and
sdist. Its version label is `dbsnp-b155-common-pgx-candidate`: the builder's
release name followed by its input scope. `candidate` means the sites were
selected from public marker lists and CPIC definition sites; the one-site
parser fixture uses `sample` instead. Every upload records the label as its
`site_table_version`. The [NOTICE](../mirobody/res/genomics/genotype_sites.NOTICE)
records source URLs and hashes, and [the builder](../benchmarks/genomics/README.md)
rebuilds the file from pinned, hash-checked inputs.

The sites were selected by the rsIDs of two openly shared PGP exports and of
the CPIC definition tables, within the twelve regions. Against those sources
the index covers 262 of 625,705 rsIDs in the PGP 23andMe v5 export, 340 of
677,436 in the PGP AncestryDNA v2 export, and 41 of 992 CPIC definition
rsIDs. Those exports are observed marker sets, not manufacturer manifests, and
these are the ratios of a twelve-region index: they do not describe
consumer-array coverage. The index has no WeGene/GSA marker set, indel
definition, merged-rsID history or rare-variant panel. A chip row outside it
is visible as a raw `unresolved` row, never as a GT. A gene query reaches only
the 41 CPIC-labelled sites.

Rows the index cannot map stay intact. The public Ancestry v2 export keeps all
27,206 of its X/Y/PAR/MT rows (25,242/1,665/36/263), and a PAR query finds
them in explicit raw-coordinate mode without claiming an assembly. A VCF
upload carries its own REF/ALT/GT, so a whole-genome VCF stores every call it
lists: the two public BGZF WGS VCFs store 4,741,304 and 5,017,551 rows. Those
calls describe the file; the index did not verify them. Import timings,
storage sizes and the full public-corpus results are in
[the benchmark notes](../benchmarks/genomics/README.md).

## Reading and exporting

`query_genetic_data` accepts no selector for an active-set overview, or one
of rsIDs (up to 50), HGNC gene or a bounded GRCh37/GRCh38 region. A region may
also use `build=raw` to search upload positions without claiming an assembly;
PAR rows require this raw mode. It returns
at most 500 direct rows with source, build, call status and truncation notes.
For a region query, `position` uses the requested build; `query_build`,
`raw_position`, `pos37` and `pos38` make its provenance explicit.
The Agent and authenticated MCP surface use the same service. A gene search
needs a catalog gene label, and an assembly-specific region needs mapped
coordinates. A `build=raw` region can find unmapped upload positions without
asserting an assembly; an unmapped row can also be found by rsID.
Care-circle reads require authorization. The tool does not infer disease risk.

`GET /api/v1/genomics/active-set` gives the Data page counts and provenance.
`GET /api/v1/genomics/export.vcf?build=GRCh38` (or `GRCh37`) streams only
mapped, defensible calls from the active set. Its header states how many rows
were omitted because they were unresolved, unmapped or unsuitable for export,
and records the upload format, vendor, declared/detected build and normalizer
and site-catalog versions. Contigs are emitted in numeric order followed by
X, Y and MT. PharmCAT 3.4.0 reads the exported VCF without warnings. From the
two public CYP2C19 sites its Named Allele Matcher can only list candidate
diplotypes, which is why Mirobody itself never names one.

`GET /api/v1/genomics/export.fhir.json?rsids=rs4244285` returns a bounded FHIR
STU3 Variant Observation collection inside the normal API envelope. It uses
the active upload, requires the same authorization as the genotype tool, and
returns `missing_rsids` and `omitted_rsids`. Only mapped called or no-call
sites with coordinates and REF/ALT can be represented. The public two-site
upload reproduces its truth through this export in all nine renderings.

## Pharmacogenomics

`query_pharmacogenomics` matches exact CPIC generic drug names or HGNC genes;
with no selector it examines active medication plans. It reads pinned CPIC
v1.60.0 A/B drug-gene relationships and counts the defining positions called,
missing or no-call in the active upload. A guideline's evidence level is not
the person's phenotype. The tool stops at coverage: it returns
`not_determined` for the phenotype, because a star allele needs phase, copy
number and every defining site, and it never issues a prescribing
recommendation. The CPIC reader maps a diplotype to a phenotype only when that
diplotype was confirmed outside the array. The bundled
[CPIC NOTICE](../mirobody/res/genomics/cpic-v1.60.0.NOTICE) records the pinned
source, extract and license.

`mirobody fetch cpic --version v1.59.1` downloads an exact public CPIC release
into `CPIC_DIR` (default `~/.mirobody/cpic`), validates its COPY schema as data,
and installs the extracted tables atomically. `--version latest` resolves the
newest official release that published a database dump at fetch time. Set `CPIC_SOURCE_URL` to a mirror's
base URL when needed. Set `CPIC_VERSION` to an installed exact version or
`latest` to select the newest **locally installed** extract for subsequent
queries; `bundled` always uses the tested packaged v1.60.0 data. Fetching a
version does not activate it. Queries read the selected version at request
time, and a PGx answer is computed per request, not stored. The pinned public
v1.59.1 and v1.60.0 dumps passed fetch, validation and version-selection tests.

Rare pathogenic array calls are not converted into disease conclusions.
Missing sites, no-calls and conflicts must not be represented as normal.

## Public-data verification and privacy

The [public-data generator](../benchmarks/genomics/generate_public_formats.py)
pins a 1000 Genomes HG00096 truth file and PharmCAT 3.4 positions by SHA-256,
then renders nine upload files from the same two calls, including gzip, BGZF,
ZIP with sidecars and both reference assemblies. The
[end-to-end check](../benchmarks/genomics/e2e_public_truth.py) exercises the
bundled site index through real WebSocket upload, active-set replacement,
rsID/gene/region MCP queries, CPIC coverage, GRCh37/38 VCF and FHIR export
and real Agent tool use. The public rs4244285 VCF lists ALT `A` while the
dbSNP site in the index lists `A,C,T`; the normalizer maps its GT index to the
catalog allele order. It is a pipeline check, not evidence of whole-chip
accuracy. A second check migrates only those public truth rows from the 1.5.1
table. No owner's genotype export or personal health document belongs in a
committed fixture.

A 40-question Agent evaluation over the same public truth checks tool choice
and forbidden claims. Qwen chose the expected tool on 37 of 40 first turns,
and the automated forbidden-claim alarm matched none. The three misses were
generic "no question" replies, counted as invalid answers. This measures tool
choice over two sites, not the clinical correctness of an answer. The
evaluation corpus and answers stay outside the repository.

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
including during context overflow recovery; public-call regression tests
cover the sync and async paths. The general-purpose subagent is disabled in
this harness. After a genetic query, the Agent refuses scratch-file reads and
writes while still allowing read-only access to the document and profile
mounts.
