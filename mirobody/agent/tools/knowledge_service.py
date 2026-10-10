"""General medical knowledge, for the chat agent and any signed-in MCP client.

These tools never read a record: they search MedlinePlus and FDA labels in a
local index, and PubMed and ClinicalTrials.gov when `KNOWLEDGE_ONLINE` is on
(`agent/knowledge/`, and the design in this directory's README). They take
`user_info` only to require a signed-in caller: with the online tier on, an
anonymous caller could otherwise use the server to query those hosts.
"""

from typing import Any

from mirobody.agent.knowledge import tools as knowledge


class MedicalKnowledgeService:
    """Listed only while this deployment has a tier (`available`)."""

    __tools__ = (knowledge.SEARCH, knowledge.READ)

    @staticmethod
    def available() -> bool:
        return bool(knowledge.scopes())

    async def search_medical_knowledge(self, query: str, user_info: dict[str, Any], scope: str = "") -> str:
        """
        Search general medical knowledge: what a lab test measures and what
        its results can mean, what a condition is, what a medicine is for, its
        side effects and interactions. Never reads the person's record.

        Args:
            query: English medical terms, e.g. "metformin side effects" or
                "high ALT". Translate the question; never include the person's
                name, values or dates.
            scope: "reference" (MedlinePlus and FDA labels, on this server, the
                default); "literature" (PubMed reviews, guidelines and trials)
                or "trials" (ClinicalTrials.gov), when the server has online
                search on.

        Returns:
            Up to five passages, each headed by its ref (e.g.
            ref:medlineplus_test:alt-blood-test), title, source and link.

        Notes for LLMs:
            - Answer a general medical question only from what this returns,
              and give each fact its source.
            - "Nothing matches" and "could not be reached" are different: the
              second is an outage, not an absence of evidence.
        """
        return await knowledge.search(query, scope)

    async def read_medical_source(self, ref: str, user_info: dict[str, Any]) -> str:
        """
        Read one passage from search_medical_knowledge in full.

        Args:
            ref: A ref exactly as a search result showed it, e.g.
                ref:medlineplus_test:alt-blood-test or ref:pmid:28304224.

        Returns:
            The passage's title, source, link and full text.
        """
        return await knowledge.read(ref)
