"""Who may read an uploaded file.

`GET /files/{key}` had no authentication of any kind. An external reviewer
fetched a real health-report PDF off a running deployment with no token, and
the object keys are `<second-resolution timestamp>_<8 hex>` — enumerable enough
to matter for a targeted attack. These tests pin the three properties that fix
depends on, because none of them is visible from reading the happy path.

No database and no object store: `execute_query` and the storage client are the
two seams and both are substituted.
"""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

# `routers/__init__` re-exports the APIRouter under the module's own name
# (`from .file_router import router as file_router`), so both
# `from ... import file_router` and `import ...file_router as fr` resolve to
# the ROUTER object, not the module. importlib is the way to reach the module
# itself, which is what monkeypatch needs.
import importlib

fr = importlib.import_module("mirobody.server.routers.file_router")

TOKEN = "Bearer test-token"


@pytest.fixture
def rows():
    """Ownership rows the substituted `execute_query` will answer with."""
    return {"th_files": [], "deep_agent_workspace": []}


@pytest.fixture
def client(monkeypatch, rows):
    async def fake_execute_query(query, params=None, *a, **kw):
        for table in ("th_files", "deep_agent_workspace"):
            if table in query:
                return rows[table]
        return []

    class FakeStorage:
        async def get(self, key):
            return b"%PDF-1.4 fake", None

        def get_content_type_from_filename(self, name):
            return "application/pdf"

    monkeypatch.setattr(fr, "execute_query", fake_execute_query)
    monkeypatch.setattr(
        "mirobody.utils.config.storage.get_storage_client", lambda *a, **kw: FakeStorage()
    )

    async def fake_verify(token_string):
        if "test-token" not in (token_string or ""):
            from fastapi import HTTPException

            raise HTTPException(status_code=401, detail="Token decode failed")
        return "42"

    monkeypatch.setattr(fr, "verify_token_string", fake_verify)

    # Deny-by-default care-circle check. The real one reaches the database
    # through its OWN `execute_query` import, so leaving it live turns an
    # expected 404 into a 500 and the test stops testing authorization.
    async def deny(user_id, query_user_id=None, permission=None):
        return {"success": False, "error": "No permission to query this user's data"}

    monkeypatch.setattr(fr, "get_query_user_id", deny)

    app = FastAPI()
    app.include_router(fr.router)
    return TestClient(app)


def test_an_anonymous_request_is_rejected(client):
    """The finding, in one line: this returned 200 and a PDF."""
    assert client.get("/files/uploads/20260818_120000_abcd1234.pdf").status_code == 401


def test_the_owner_can_read_their_own_file(client, rows):
    rows["th_files"] = [{"user_id": "42"}]
    r = client.get(
        "/files/uploads/20260818_120000_abcd1234.pdf", headers={"Authorization": TOKEN}
    )
    assert r.status_code == 200
    assert r.content.startswith(b"%PDF")


def test_a_token_in_the_query_string_also_works(client, rows):
    """A browser cannot set a header on a navigation, an <img src> or a PDF
    embed, and `LocalStorage._build_url` hands out exactly such URLs. Without
    this the route authenticates but the feature it serves cannot."""
    rows["th_files"] = [{"user_id": "42"}]
    r = client.get("/files/uploads/x.pdf?access_token=Bearer%20test-token")
    assert r.status_code == 200


def test_another_users_file_is_not_readable(client, rows):
    """Authenticated is not authorized — the whole point of the ownership row."""
    rows["th_files"] = [{"user_id": "99"}]
    r = client.get("/files/uploads/x.pdf", headers={"Authorization": TOKEN})
    assert r.status_code == 404


def test_a_care_circle_member_can_read_a_shared_file(client, rows, monkeypatch):
    """Sharing must reach the bytes, not only the listing, or a shared report
    shows in the file list and 404s when opened."""
    rows["th_files"] = [{"user_id": "99"}]

    async def allowed(user_id, query_user_id=None, permission=None):
        return {"success": True, "query_user_id": query_user_id, "permissions": {}}

    monkeypatch.setattr(fr, "get_query_user_id", allowed)
    r = client.get("/files/uploads/x.pdf", headers={"Authorization": TOKEN})
    assert r.status_code == 200


def test_an_unrecorded_key_is_denied(client):
    """No ownership row means nobody can vouch for it. Denying by default is
    what keeps agent artifacts and stray objects from being world-readable."""
    r = client.get("/files/uploads/unknown.pdf", headers={"Authorization": TOKEN})
    assert r.status_code == 404


def test_a_denied_read_is_indistinguishable_from_a_missing_file(client, rows):
    """403 on someone else's key confirms the key exists, which restores the
    enumeration oracle this fix removes. Both answers must be 404."""
    rows["th_files"] = [{"user_id": "99"}]
    other = client.get("/files/uploads/real.pdf", headers={"Authorization": TOKEN})
    missing = client.get("/files/uploads/nope.pdf", headers={"Authorization": TOKEN})
    assert other.status_code == missing.status_code == 404
    assert other.json() == missing.json()


def test_phi_is_not_left_in_shared_caches_or_rendered_inline(client, rows):
    """`Cache-Control: public` on a now-per-user response would let a shared
    proxy serve one person's labs to the next caller; `inline` on an uploaded
    .html or .svg executes it on this origin."""
    rows["th_files"] = [{"user_id": "42"}]
    r = client.get("/files/uploads/x.pdf", headers={"Authorization": TOKEN})
    assert "public" not in r.headers["cache-control"]
    assert r.headers["content-disposition"].startswith("attachment")
