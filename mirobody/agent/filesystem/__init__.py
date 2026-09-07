"""The agent's virtual filesystem — what `ls` and `read_file` see.

Every mount is a READ-ONLY PROJECTION of a table the product already owns.
They used to be copies: `/uploads/` and `/library/` were rows in a
`deep_agent_workspace` table that a sync pass rewrote on every turn, and
`/memories/health_profile.md` was a mirror of the profile document. A
projection cannot outlive what it projects — which is what a copy did, and how
a deleted lab report kept answering questions (1.2.1).

    backend.py           how a stored file READS: text inline, a binary as a
                         content block, a big one through object storage
    files_backend.py     `/uploads/` and `/library/` over `th_files`
    profile_backend.py   `/memories/` over the health profile
    document_backend.py  one read-only file over any document produced on
                         demand — the protocol, and the two rules production
                         taught (an empty document lists nothing; a missing one
                         answers exactly `file_not_found`)
    naming.py            what a file is CALLED and what KIND it is
    parser.py            upload-time preparation
    coercion.py          an LLM's arguments to a filesystem tool, coerced

`document_backend` and `naming` are the library half: a consumer running its own
agent binds them and gets the same naming and the same `file_not_found`.
"""
