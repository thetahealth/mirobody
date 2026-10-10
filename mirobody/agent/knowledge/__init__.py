"""General medical knowledge for the agent: one capability, two tiers.

The offline tier (`offline`, built by `build`) is an FTS5 index of MedlinePlus
and FDA labels; the online tier (`online`) searches Europe PMC and
ClinicalTrials.gov when `KNOWLEDGE_ONLINE` is on. `tools` exposes both as
`search_medical_knowledge` / `read_medical_source`, agent-only: the MCP surface
stays the record tools. The design is in `agent/tools/README.md`.
"""
