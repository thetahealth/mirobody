"""The platform-shaped records surface: wire shapes, scopes, and standardization.

No database and no LLM key: `execute_query` and `parse_text` are the two seams,
and both are substituted. What is being pinned is the CONTRACT — the field names
a developer reads off docs.mirobody.ai and then finds on their own deployment —
plus the two places this surface deliberately departs from the hosted one.
"""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from mirobody.server.auth import verify_token
from mirobody.server.routers import records_router as rr


@pytest.fixture
def calls():
    """Captured (sql, params) pairs from the substituted execute_query."""
    return []


@pytest.fixture
def client(monkeypatch, calls):
    async def fake_execute_query(query, params=None, *a, **kw):
        calls.append((query, params))
        if "DELETE" in query:
            return [{"id": 1}, {"id": 2}]
        if query.strip().upper().startswith("SELECT") or "SELECT" in query.split("\n")[1].upper():
            return []
        return []

    monkeypatch.setattr(rr, "execute_query", fake_execute_query)

    app = FastAPI()
    app.include_router(rr.router)
    app.dependency_overrides[verify_token] = lambda: "42"
    return TestClient(app)


# ── standardization ─────────────────────────────────────────────────────────

def test_standardize_wire_shape(client, monkeypatch):
    """The response names its fields the way the hosted endpoint does."""
    from mirobody.engine import Reading, resolve

    async def fake_parse_text(text, **kw):
        return [Reading(name="空腹血糖", value="5.6", unit="mmol/L",
                        reference_range="3.9-6.1", resolution=resolve("空腹血糖"))]

    monkeypatch.setattr("mirobody.engine.parse_text", fake_parse_text)
    r = client.post("/api/standardize", json={"text": "空腹血糖 5.6 mmol/L"})
    assert r.status_code == 200
    body = r.json()
    assert body["object"] == "extraction"
    assert body["stored"] is False and body["stored_count"] == 0
    row = body["data"][0]
    assert set(row) == {
        "indicator_raw", "canonical_name", "loinc_code", "value_raw",
        "parsed_value", "unit_raw", "unit_ucum", "reference_range", "measured_at",
    }
    assert row["indicator_raw"] == "空腹血糖"
    # 14771-0 "Fasting glucose [Moles/volume]", and every part of that is load-
    # bearing. Not 2339-0 (plain Glucose — fasting is its own concept), and not
    # 1558-6 (the [Mass/volume] variant) because this reading is in mmol/L and
    # LOINC codes the unit into the identity. The route has the unit, so filing
    # it under the mg/dL code would be a mixed-unit series by construction.
    assert row["loinc_code"] == "14771-0"         # resolved offline, not guessed
    assert row["parsed_value"] == "5.6"
    assert row["unit_ucum"] == "mmol/L"


def test_standardize_narrative_text_is_not_an_error(client, monkeypatch):
    """Prose with no reading in it is a documented boundary, not a 4xx."""
    async def fake_parse_text(text, **kw):
        return []

    monkeypatch.setattr("mirobody.engine.parse_text", fake_parse_text)
    r = client.post("/api/standardize", json={"text": "dizzy all afternoon"})
    assert r.status_code == 200
    assert r.json()["data"] == []
    assert "note" in r.json()


def test_standardize_without_an_llm_key_says_so(client, monkeypatch):
    """A deployment with no provider key gets the message, not a stack trace."""
    async def fake_parse_text(text, **kw):
        raise RuntimeError("no LLM provider key is configured")

    monkeypatch.setattr("mirobody.engine.parse_text", fake_parse_text)
    r = client.post("/api/standardize", json={"text": "glucose 5.6"})
    assert r.status_code == 400
    err = r.json()["error"]
    assert err["code"] == "extraction_failed"
    assert "provider key" in err["message"]


def test_store_true_writes(client, monkeypatch, calls):
    from mirobody.engine import Reading, resolve

    async def fake_parse_text(text, **kw):
        return [Reading(name="HGB", value="140", unit="g/L", reference_range="",
                        resolution=resolve("HGB"))]

    monkeypatch.setattr("mirobody.engine.parse_text", fake_parse_text)
    r = client.post("/api/standardize", json={"text": "HGB 140 g/L", "store": True})
    assert r.json()["stored"] is True and r.json()["stored_count"] == 1
    sql, params = calls[-1]
    assert "INSERT INTO th_series_data" in sql
    assert params[0]["source"] == "standardize"
    # HGB used to answer 4548-4 (HbA1c); the row it writes must carry hemoglobin.
    assert params[0]["indicator_id"] == "718-7"


# ── records ─────────────────────────────────────────────────────────────────

def test_write_records_standardizes_on_the_way_in(client, calls):
    r = client.post("/api/data", json={"records": [
        {"indicator": "fasting_glucose", "value": 5.6, "unit": "mmol/L",
         "time": "2026-08-19T07:30:00Z"},
        {"indicator": "no such indicator at all", "value": 1},
    ]})
    assert r.status_code == 200
    assert r.json() == {"status": "ok", "ingested": 2, "standardized": 1}
    _, params = calls[-1]
    # snake_case is the spelling the platform docs teach; it must reach a code —
    # and the mmol/L variant of it, see test_standardize_wire_shape.
    assert params[0]["indicator_id"] == "14771-0"
    assert params[1]["indicator_id"] == ""      # honest miss, still stored
    assert params[0]["value"] == "5.6 mmol/L"


def test_retention_and_session_id_are_accepted_and_ignored(client):
    """A caller copying a hosted example must not get a 400 for a field we
    have no use for — the deployment has no expiry scheduler behind it."""
    r = client.post("/api/data", json={
        "records": [{"indicator": "steps", "value": 12450}],
        "retention": "permanent",
        "session_id": "sess_abc",
    })
    assert r.status_code == 200


def test_write_caps_the_batch(client):
    r = client.post("/api/data", json={
        "records": [{"indicator": "steps", "value": 1}] * (rr.MAX_RECORDS_PER_REQUEST + 1)
    })
    assert r.status_code == 422


def test_read_records_list_envelope(client, monkeypatch, calls):
    from datetime import datetime

    async def rows(query, params=None, *a, **kw):
        calls.append((query, params))
        return [{
            "id": 7, "indicator": "空腹血糖", "value": "5.6 mmol/L",
            "start_time": datetime(2026, 8, 19, 7, 30), "end_time": None,
            "source": "api", "comment": "garmin", "indicator_id": "2339-0",
        }]

    monkeypatch.setattr(rr, "execute_query", rows)
    r = client.get("/api/data?limit=10")
    body = r.json()
    assert body["object"] == "list" and body["has_more"] is False
    row = body["data"][0]
    assert row["id"] == 7 and row["loinc_code"] == "2339-0"
    assert row["parsed_value"] == "5.6" and row["parsed_unit"] == "mmol/L"
    assert row["time"] == "2026-08-19T07:30:00"


def test_delete_requires_an_explicit_scope(client):
    """Unlike the hosted endpoint, "no filter" is NOT "everything" here.

    Behind an API key an operator minted, a bare DELETE meaning "all" is
    defensible. On a self-hosted box a mistyped curl is one keystroke from a
    person's entire record, so the widest scope has to be asked for.
    """
    r = client.request("DELETE", "/api/data")
    assert r.status_code == 400
    assert r.json()["error"]["code"] == "missing_scope"

    assert client.request("DELETE", "/api/data?id=7").status_code == 200
    assert client.request("DELETE", "/api/data?all=true").status_code == 200


def test_read_does_not_decrypt_the_plaintext_column(client, calls):
    """`value` is stored in the clear; `comment` is not.

    Every other reader in the repo selects `tsd.value` raw, and all three
    writers wrap `comment` in `encrypt_content`. Getting this backwards hands a
    caller ciphertext for a lab value, which reads as data corruption.
    """
    client.get("/api/data")
    sql, _ = calls[-1]
    assert "decrypt_content(value)" not in sql
    assert "decrypt_content(comment)" in sql


def test_delete_is_scoped_to_the_caller(client, calls):
    client.request("DELETE", "/api/data?id=7")
    sql, params = calls[-1]
    assert "user_id = :uid" in sql and params["uid"] == "42"


def test_errors_use_the_platform_envelope(client):
    r = client.request("DELETE", "/api/data")
    err = r.json()["error"]
    assert set(err) == {"message", "type", "code", "param"}
    assert err["type"] == "invalid_request_error"
