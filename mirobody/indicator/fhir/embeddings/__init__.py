"""FHIR embedding bundle: produce, load, fix-up.

  - :mod:`db`        producer when ``fhir_indicators`` is populated (compat
                     mode — also emits the ``fhir_id_map.npy`` sidecar)
  - :mod:`ref`       producer from ~/ref source files (terminal mode — no
                     id_map; upstream must populate ``th_series_data.fhir_id``
                     with canonical packed values)
  - :mod:`local`     consumer: lazy loader + dtype/path constants
  - :mod:`bundle`    tar.gz read/write API for the shared resolver
                     bundle (axes, masks, aliases, dose index, …)
  - :mod:`names`     display-name parsers (LOINC LCN / SNOMED FSN / RxNorm
                     best-TTY / CVX) used inline by both producers; the
                     ``code-names`` subcommand is a recovery path for
                     bundles whose ``name`` column was left empty
  - :mod:`migrate`   one-shot recovery: convert legacy 4-file artifacts to
                     the new structured-npy + meta + id_map layout
  - :mod:`skip`      build ``fhir_loinc_skip.npy`` row-aligned mask so
                     resolve drops PHENX/SURVEY/DOC class codes; loaded
                     eagerly by :mod:`local` (~700 KB) and applied in
                     ``FhirAdapter._resolve_local_batch``
  - :mod:`alias`     build ``loinc_alias_index.npz``: LOINC's own per-
                     language synonyms inverted to a row index, applied
                     as a +0.04 cosine bonus on lexical match
  - :mod:`lexicon`   build per-language ``aliases/{lang}.tsv``: bridges
                     gaps the embedding can't close on its own (rare
                     Latin binomials, drug INNs, Japanese kana). LOINC
                     LinguisticVariant for zh / ko / fr / es / ru /
                     de; UMLS MRCONSO for ja
  - :mod:`preprocess` query-side augment that consumes the lexicon
                     above — runs BEFORE the embedding call, appends
                     canonical EN to non-English clinical phrases
  - :mod:`dose`      ``fhir_dose_index.npz``: (value, UCUM unit) →
                     corpus rows, applied at resolve time when the
                     query carries an explicit dose specifier
  - :mod:`rank`      ``loinc_rank_bonus.npy``: COMMON_TEST_RANK tier
                     bonus (≤ 0.020) — common-test tie-breaker
"""
