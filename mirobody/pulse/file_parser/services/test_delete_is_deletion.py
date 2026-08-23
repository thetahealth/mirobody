"""Deleting a health document must not leave the agent a copy to answer from.

Deleting a lab report removes the object from storage, soft-deletes the
`th_files` row and cascade-deletes the readings — but the agent reads through
two more surfaces, and a copy surviving in either means the UI shows the file
gone while the model still quotes all twelve values:

* the agent's virtual filesystem — its mounts are projections of the owning
  tables (`deep/files_backend.py`, `deep/profile_backend.py`), so
  soft-deleting the `th_files` row IS the deletion there, in the same query
  the agent reads through;
* the health profile's `/memories/` mirror — a DERIVED summary the LLM wrote,
  quoting readings verbatim and carrying no `file_key`. Not a view, so it is
  the one thing a projection cannot fix for itself: it has to be invalidated
  explicitly — and for the OWNER of the readings, who on a care-circle upload
  is not the person who uploaded the file.
"""

from __future__ import annotations

import pytest

from mirobody.pulse.file_parser.services import file_processing_service as svc


@pytest.fixture
def sql(monkeypatch):
    """Capture every statement the delete path issues, with its params."""
    calls: list[tuple[str, dict]] = []

    async def fake(query=None, params=None, *a, **kw):
        calls.append((" ".join(str(query).split()), dict(params or {})))
        return [{"n": 1}]

    monkeypatch.setattr(svc, "execute_query", fake)
    return calls


async def test_the_derived_profile_is_invalidated_not_repaired(sql):
    await svc._invalidate_derived_profile("5")
    stmts = [q for q, _ in sql]
    assert any(s.startswith("UPDATE health_user_profile_by_system") and "is_deleted = true" in s
               for s in stmts), "the profile row survived"
    assert all(p.get("user_id") == "5" for _, p in sql)


async def test_invalidating_the_source_is_the_whole_job(sql):
    """One statement, not two — because nothing mirrors the profile.

    The copy at `/memories/health_profile.md` is served by
    `deep/profile_backend.ProfileBackend`, which selects the profile with
    `is_deleted = false`. A second statement here would mean a mirror exists
    again — a copy the delete path has to chase.
    """
    await svc._invalidate_derived_profile("5")
    assert len(sql) == 1, f"expected one statement, got {[q[:40] for q, _ in sql]}"
    assert "deep_agent_workspace" not in sql[0][0]


async def test_invalidation_is_a_no_op_without_an_owner(sql):
    await svc._invalidate_derived_profile("")
    assert sql == []


async def test_a_failure_is_logged_loudly_and_never_swallowed(monkeypatch, caplog):
    """Silent failure means the UI shows the file gone and the model still knows it."""
    async def boom(*a, **kw):
        raise RuntimeError("db down")
    monkeypatch.setattr(svc, "execute_query", boom)
    with caplog.at_level("ERROR"):
        await svc._invalidate_derived_profile("20")
    assert any("FAILED" in r.message for r in caplog.records)


@pytest.fixture
def delete_path(monkeypatch):
    """Substitute everything the delete path touches, recording the invalidation."""
    seen: dict = {"invalidated": []}

    class FakeDb:
        @staticmethod
        async def get_file_by_key(file_key, user_id):
            # query_user_id differs from user_id: a care-circle upload, where the
            # readings belong to the member and the file to the uploader.
            return {"file_name": "r.pdf", "file_type": "report",
                    "scene": "report", "query_user_id": "5"}

        @staticmethod
        async def soft_delete_file(file_key, user_id):
            return True

    import mirobody.pulse.file_parser.services.file_db_service as db_mod
    monkeypatch.setattr(db_mod, "FileDbService", FakeDb)

    async def fake_storage(file_key=None, **kw):
        return True

    async def fake_invalidate(owner_id):
        seen["invalidated"].append(owner_id)

    monkeypatch.setattr(svc, "delete_file_from_storage", fake_storage)
    monkeypatch.setattr(svc, "_invalidate_derived_profile", fake_invalidate)
    monkeypatch.setattr(svc, "_start_background_cascade_delete", lambda *a, **kw: None)
    return seen


async def test_deleting_a_file_invalidates_the_owner_s_profile(delete_path):
    out = await svc.delete_files_from_message("msg-1", ["web_uploads/a.pdf"], "20")
    assert out["success"] is True
    assert delete_path["invalidated"] == ["5"], (
        "the profile was not invalidated, or was invalidated for the uploader "
        "instead of the owner of the readings"
    )


async def test_the_result_reports_no_agent_copy_revocation(delete_path):
    """The result must not report an `agent_copies_revoked` count — there is no
    agent copy to revoke. This key appearing means a mirror exists again.
    """
    out = await svc.delete_files_from_message("msg-1", ["web_uploads/a.pdf"], "20")
    assert "agent_copies_revoked" not in out["deleted_files"][0]
