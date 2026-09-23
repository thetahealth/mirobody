# Genetics

> **Status: 1.5.2, in progress.** Sections marked **(today)** describe code on
> this branch. Sections marked **(1.5.2)** are the design the release is built
> to, each with the test that will hold it. Every **(1.5.2)** marker comes off,
> or the section goes, before the branch merges.

A genotype is **not a reading**, for the same reason a medication is not
([medications.md](medications.md)): it has no time, no unit and no value to
aggregate. "What did this person's array call at rs4244285" has one answer
that does not change, and a trend of it means nothing. So genetics has its own
table, its own normalisation and its own tools.

Standardising a genotype is also a different job from standardising a lab
value. An indicator's hard problem is its *name* (a hundred spellings of one
test, resolved to one LOINC code). A genotype's name is already a code, the
dbSNP rsID. Its hard problems are the **coordinate system** (which reference
build, which strand, which allele is REF) and the **value** (`AG` or `GA`, and
whether `--` means "not read", "not applicable" or "not on the chip").

---

## What gets in

### Raw genotype exports (today)

Recognised by the file's **column header**, never by the vendor's banner, so a
file from an unlisted vendor with a standard header works too:

| Layout | Header | Vendors seen |
| --- | --- | --- |
| one genotype column | `rsid  chromosome  position  genotype` (commented or not) | WeGene, 23andMe, 23Mofang, Genes for Good, SelfDecode, tellmeGen |
| two allele columns | `rsid  chromosome  position  allele1  allele2` | AncestryDNA |
| CSV, one result column | `RSID,CHROMOSOME,POSITION,RESULT` | MyHeritage, FTDNA |

A text file without such a header is refused, even when it is full of
rs-numbers: a note that mentions rs4988235 is not a genotype export, and routing
it into the genetic pipeline would swallow it without saving anything.

No-calls are stored in one spelling, `--`, whatever the vendor wrote (`--`,
`00`, `0` per allele). Half a call (one allele read, one not) is a no-call.

### Also recognised (1.5.2)

A format registry (a table, not code branches) adds:

- **compressed downloads**: 23andMe ships a `.zip`, FTDNA a `.csv.gz`;
- **VCF**, from sequencing and from arrays exported as VCF;
- Illumina GenomeStudio reports (`[Header]` / `[Data]`), CircleDNA,
  Mapmygenome, LivingDNA, FTDNA Family Finder;
- vendor chromosome spellings: AncestryDNA writes 23/24/25/26 for X/Y/PAR/MT,
  FTDNA writes `0` and `XY`.

Illumina **TOP**-strand files are recognised but their alleles are marked
`unresolved`: TOP is not the plus strand, and converting it needs the chip's
manifest. It is not guessed.

### Genetic test reports (1.5.2)

A hospital pharmacogenomic report (CYP2C19 before clopidogrel, MTHFR in
pregnancy care, CYP2C9 + VKORC1 before warfarin) is a document, and goes
through the same extraction as a lab report. The LOINC codes exist in the
bundle (`57132-3` CYP2C19 gene allele, `104667-1` CYP2C19 activity score,
`13299-3` HLA-B type); 1.5.2 adds the report phrasings that reach them and
normalises the values (star alleles, CPIC phenotype terms).

---

## What gets normalised (1.5.2)

Each row is joined to a **site table** (dbSNP positions on GRCh37 and GRCh38,
REF/ALT, gene) built from the union of consumer-array sites and shipped as a
release asset. The original genotype is kept as written; normalisation adds:

| Field | Meaning |
| --- | --- |
| `gt` | VCF genotype relative to REF: `0/0` `0/1` `1/1` `1/2`; haploid `0` / `1` on male X/Y and on MT |
| `call_status` | `called`, `no_call` (the array tried and could not read it), `not_applicable` (a Y site in a female sample), `unresolved` (no site-table entry, a strand conflict, an undefined insertion) |
| zygosity | heterozygous / homozygous / hemizygous, as LOINC answer codes (LA6706-1, LA6705-3, LA6707-9) |
| position | GRCh38, plus the build the file was written on |
| rsid | the current dbSNP number; a merged-away number is mapped and noted |

Per upload, the build is **detected** from known marker positions, not taken
from the banner, and sex is inferred from X heterozygosity and the Y call rate
(`unknown` when the two disagree). A real WeGene export from a female donor
carries 24,311 Y sites, all `--`: those are `not_applicable`, not 24,311
failed reads.

Four "empties" are kept apart, because each licenses a different sentence:
**absent** (the array does not type this site), **no call**, **not
applicable**, **unresolved**. None of them means "normal".

Export follows the standards: VCF (GRCh37 as uploaded, GRCh38 lifted), and
each site expressible as an HL7 FHIR Genomics Variant observation (LOINC
69548-6, with 62374-4 for the build and 53034-5 for zygosity).

---

## How it is stored (1.5.2)

One **genotype set** per upload, with its format, vendor, declared and
detected build, call rate, inferred sex and status. Queries read only the
person's `active` set. A new upload loads into a `loading` set, is checked
(rows stored equal rows parsed), then replaces the active one in a single
transaction. A partly written genome is never visible, and uploading the same
file twice does not double it.

Today (on 1.5.0 and this branch) the rows go into `th_series_data_genetic`
with no set: a re-upload duplicates every row, and a failed batch is skipped
while the load still reports success. The set model fixes both.

---

## The two tools

| | `query_genetic_data` | `query_pharmacogenomics` (1.5.2) |
| --- | --- | --- |
| Answers | "what does my genotype file say" | "does this drug interact with my genes" |
| Kind | **fact**: reads the genotype back, interprets nothing | **interpretation**: combines sites into a CPIC result |
| Input | nothing (an overview of the upload), or `rsids` / `genes` / `region` | `drugs` (any language) or `genes`; nothing = check the person's current medications |
| Output | per site: gene, position, genotype as written, `gt`, zygosity, `call_status` | per drug: gene, diplotype (`*1/*2`), phenotype (Intermediate Metabolizer), CPIC level, the guideline's recommendation, which defining sites were and were not typed |
| Reads | the active genotype set | results computed at upload, plus genetic results extracted from reports |

They are two tools because they answer questions with different grammar and
different standing. One returns data; the other returns a conclusion drawn
from it by published rules. Folding them together leaves the model unable to
tell which part of an answer is the person's data and which part is inference.

Neither tool ever returns the whole genotype: a call returns the sites asked
about, capped. Both start every answer with the provenance of the set (vendor,
build, upload date, inferred sex, call rate).

### What pharmacogenomics can and cannot do

CPIC guidelines are written against **diplotypes and phenotypes**, not single
sites. CYP2C19 before clopidogrel means combining a dozen defining sites into
`*1/*2`, reading that as an intermediate metaboliser, and applying the
guideline's recommendation. That is deterministic, so it runs at upload, in
code, against a pinned CPIC version, not in the model.

Of the 97 drugs with a CPIC level A or B recommendation (CPIC data, September
2026), **46** depend on a gene an array can call: clopidogrel, warfarin, the
statins, the proton-pump inhibitors, the thiopurines, fluorouracil and
capecitabine, tacrolimus, irinotecan, several NSAIDs and SSRIs. The other
**51** depend on genes an array cannot type (CYP2D6, HLA, MT-RNR1, G6PD,
RYR1), including allopurinol (HLA-B\*58:01) and carbamazepine (HLA-B\*15:02).
For those the tool says the chip cannot answer and which test would, rather
than staying silent.

A result always lists the defining sites that were not typed. A missing site
is never filled in as the reference allele: a rarer allele there cannot be
excluded, so the call is "not determined", not `*1`.

---

## Keeping CPIC current (1.5.2)

CPIC data changes, and the changes move results. In the twelve months to
September 2026, CPIC published 12 data releases (v1.53.0 to v1.60.1) and 29
allele-definition or function changes in the genes an array can call. Among
them: NUDT15 `*2` became a sub-allele of `*3` (2026-01-29), six SLCO1B1
haplotypes were deleted (2025-10-14), and the CYP2C19 `*2` core definition
changed (2026-08-03). A snapshot frozen into a release goes stale within months.

So the CPIC version is **configuration**, like a model name:

| Setting | Default | Meaning |
| --- | --- | --- |
| `CPIC_VERSION` | `bundled` | `bundled` (the snapshot this release was tested against), an exact release such as `v1.60.0`, or `latest` |
| `CPIC_DIR` | `~/.mirobody/cpic/` | where fetched versions are kept, one directory per version |
| `CPIC_SOURCE_URL` | `https://files.cpicpgx.org/data/database/` | where versions are fetched from; point it at a mirror if that host is not reachable from your network |
| `CPIC_STALE_DAYS` | `180` | `mirobody doctor` warns when the version in use is older than this |

```bash
mirobody fetch cpic                  # the latest CPIC release
mirobody fetch cpic --version v1.60.0
mirobody doctor                      # version in use, its date, and whether a newer one exists
```

What `fetch` does, and what it does not:

- It downloads CPIC's versioned database export (`cpic_db_dump-<version>.sql.gz`,
  about 4 MB; old versions stay available) and **reads its data blocks into
  mirobody's own tables. It never executes the downloaded SQL.**
- It checks the export's schema version and refuses one it does not
  understand, leaving the version in use unchanged.
- Switching versions **recomputes** every stored pharmacogenomic result in the
  background. Old results are kept with the version that produced them, so an
  answer can say "this changed with CPIC v1.61.0" instead of silently giving a
  different phenotype.
- Nothing fetches at query time. A query sends no drug name or genotype
  anywhere, the stack runs offline, and the same version always gives the same
  answer.

CPIC content is CC0. Attribution to CPIC is requested, and every answer names
the version it used.

---

## What this does not do

| Not done | Why |
| --- | --- |
| Disease risk from rare pathogenic variants | Arrays are unreliable exactly there. In 49,908 UK Biobank participants, array-reported BRCA pathogenic variants had a **positive predictive value of 4.2%**, and 84% of calls for variants rarer than 1 in 100,000 were false positives (Weedon et al., BMJ 2021). A ClinVar pathogenic site is shown as a site, with that caveat and the advice to confirm by clinical sequencing, never as a risk. |
| Polygenic risk scores | Scores trained on European cohorts lose about half their accuracy in East Asian populations (Martin et al., Nat Genet 2019). |
| CYP2D6, HLA typing | Copy number and HLA diversity are beyond an array. |
| Ancestry, relatives, trait "fun facts" | Out of scope. SNPedia's content is CC BY-NC-SA, which a hosted deployment cannot use. |
| PharmGKB content in the package | CC BY-SA would pull the package's data under share-alike. PharmGKB is linked, not bundled. |

## Knowledge sources

| Source | Used for | Licence |
| --- | --- | --- |
| dbSNP | site table: positions, REF/ALT, merged rsIDs | public domain |
| CPIC | allele definitions, phenotypes, recommendations | CC0 |
| ClinVar | labelling sites, never conclusions | public domain |
| gnomAD v4 | population frequency for the rare-variant rule | CC0 |
| HGNC | gene symbols | CC0 |

## Privacy

A genotype is sensitive personal information under most regimes (China's PIPL
lists it in Art. 28, and Art. 29 requires separate consent to process it). This stack keeps it in your
database and never sends it in bulk to a model: each tool call returns only the
sites it was asked for. If you run a hosted deployment, human genetic data may
carry further obligations where you operate; that is a legal question this page
does not answer.

## Testing

The pipeline is tested against public data with known answers: vendor format
fixtures from the [snps](https://github.com/apriha/snps) project (BSD-3), the
1000 Genomes Project and the Genome in a Bottle Han Chinese trio (HG005/6/7)
for genotypes, and the CDC GeT-RM consensus genotypes for pharmacogenomic calls
(173 of its 363 reference samples are 1000 Genomes samples, 47 of them East
Asian). The array files used to score normalisation are generated from those
genomes, so they can be published; no real person's upload is in the test data.
