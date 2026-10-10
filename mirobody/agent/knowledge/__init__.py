"""General medical knowledge for the agent: one capability, two tiers.

The offline tier (`offline`, built by `build`) is an FTS5 index of MedlinePlus
and FDA labels; the online tier (`online`) searches Europe PMC and
ClinicalTrials.gov when `KNOWLEDGE_ONLINE` is on. `tools` answers
`search_medical_knowledge` / `read_medical_source`, which
`agent/tools/knowledge_service.py` offers the chat agent and signed-in MCP
clients. The design is in `agent/tools/README.md`.
"""
