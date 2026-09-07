"""The kernel: what health data *means*, as pure functions. stdlib + numpy.

A reading travels through the reference application in `pulse/` (collect,
store, aggregate) and is answered from `agent/`; this package is the part of
that path that is not a database or a model call — the rules, and only the
rules, so any other application can run the same ones. Nothing here opens a
connection, reads a clock it was not handed, or imports a third-party package
(import-linter enforces it: `pyproject.toml`, four contracts).

What a reading passes through, and which module decides each step:

    vendor payload ─ vendors.decode ──▶ series.Fact      the vendor's JSON becomes facts
    Fact ─────────── metrics ──────────▶ shape            what this metric IS: state_class,
                                                          aggregation policy, canonical unit,
                                                          local-day window (res/metrics.tsv)
    Fact ─────────── quality ──────────▶ admit / reject   only the impossible is rejected,
                                                          with a reason code (pulse/readings.py)
    facts of a day ─ series.aggregate ─▶ one number       deltas summed, spans unioned,
                                                          provider dailies projected
    sources of a day series.elect ────▶ one authority     measurer > echo, coverage, freshness
    stored row ───── overlay ──────────▶ corrected view   a correction is a layer, never a rewrite
    request ──────── query ────────────▶ one tool         `query_health_indicators`: schema,
                                                          dispatch, window, envelope
    tool result ──── tools ────────────▶ envelope         status, fault class, retry ledger
    log line ─────── ops ──────────────▶ ids and counts   never a value

Beside the reading path: `meds` (medications are an entity — plans, doses,
adherence — not readings; their own tool, `query_medications`), `connect` (what a credential and a pull promise),
`sink` (what writing a row twice means), `events` (the wire-neutral agent
event vocabulary a consumer's own runtime emits and its adapters translate),
`evidence` (statistics produce the evidence a model only narrates: deviation /
trend / recovery / gap detection under an injected policy, and the narrative
rules a summary must obey), `memory` (the profile document a rewrite
regenerates and an agent reads, with the watermark that says what is fresh),
and `vendors/` (one decode table per vendor, with samples shipped for a
consumer to run against its own decoder).

The vocabulary layer — a lab name to a LOINC code — is the sibling
`mirobody.engine` / `mirobody.units` / `mirobody.lexical`, older and larger;
the kernel builds on `units` for dimensions and conversions.
"""
