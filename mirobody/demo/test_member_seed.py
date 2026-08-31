"""Every sign-in account gets its OWN thin record beside the shared one.

The walkthrough's point is data isolation you can SEE: ask about your HbA1c
and one boring-normal value answers from your own record; ask about hers and
a two-year story answers from a record you merely have view access to. That
only lands if the sign-in account actually owns data — an empty account
demotes the isolation from something on the screen to a sentence in the
README.

No database: `execute_query` and the care-circle calls are the seams and are
substituted.
"""

from __future__ import annotations

import pytest

import mirobody.demo as demo


def test_member_series_is_thin_normal_and_tells_the_contrast_story():
    rows = demo._member_series("42", "caregiver@mirobody.ai")

    # Thin — an order of magnitude away from the shared record's 14,273.
    assert 15 <= len(rows) <= 60

    by_indicator: dict[str, list] = {}
    for r in rows:
        assert r["user_id"] == "42"
        assert r["start_time"] and r["end_time"]
        by_indicator.setdefault(r["indicator"], []).append(r)

    # The isolation contrast turns on the SAME indicator the shared record's
    # storyline uses — hers 7.2→6.5→6.6, the member's boring and normal.
    hba1c = by_indicator.get("GlycatedHemoglobin-HbA1c")
    assert hba1c, "the member record must include its own HbA1c"
    assert all(4.5 <= float(r["value"]) <= 5.6 for r in hba1c), (
        "the member's HbA1c must be NORMAL — the contrast is the demo's point"
    )

    # Deterministic: replays must upsert the same rows, not add new ones.
    assert rows == demo._member_series("42", "caregiver@mirobody.ai")


async def test_seed_gives_each_member_their_own_data(monkeypatch):
    # The sibling test above runs on a bare `pip install mirobody`; this one
    # monkeypatches `utils.execute_query` and `user.care_circle`, so it needs
    # the parse and server stacks. Skipped here rather than gated in conftest,
    # because a whole-file ignore would take the bare-install test with it.
    # (`dotenv` is what is missing first: `mirobody.utils` imports config,
    # which imports it at module scope.)
    pytest.importorskip("dotenv", reason="mirobody.utils needs the [parse] stack")
    pytest.importorskip("psycopg_pool", reason="care_circle needs the [app] stack")

    calls = {"series_batches": [], "files": [], "users": []}

    async def fake_execute_query(sql, params=None, log_sql=True, **kw):
        if "INSERT INTO health_app_user" in sql:
            calls["users"].append(params["email"])
            # owner first (id 1), then members (id 2, 3, ...)
            return [{"id": len(calls["users"])}]
        if "SELECT id FROM health_app_user" in sql:
            return [{"id": 999}]
        if "INSERT INTO th_series_data" in sql:
            calls["series_batches"].append(params)
            return {"record_count": len(params)}
        if "INSERT INTO th_files" in sql:
            calls["files"].append(params)
            return {"record_count": 1}
        raise AssertionError(f"unexpected SQL: {sql[:60]}")

    import mirobody.utils as utils_mod
    monkeypatch.setattr(utils_mod, "execute_query", fake_execute_query)

    import mirobody.user.care_circle as cc

    async def _noop(*a, **kw):
        return 7
    for fn in ("ensure_own_circle", "set_health_access", "invite", "respond_to_invitation"):
        monkeypatch.setattr(cc, fn, _noop)

    # Blob writes go to real storage; substitute so the test stays hermetic.
    blobs = []

    async def fake_put_blob(file_key, text):
        blobs.append(file_key)
    monkeypatch.setattr(demo, "_put_blob", fake_put_blob)

    await demo.seed(["caregiver@mirobody.ai"])

    # The member (user id 2) got a series batch and a document of their own,
    # distinct from the owner's (user id 1) fixture data.
    member_batches = [
        b for b in calls["series_batches"]
        if isinstance(b, list) and b and b[0]["user_id"] == "2"
    ]
    assert member_batches, "member got no readings of their own"
    member_files = [f for f in calls["files"] if f["user_id"] == "2"]
    assert member_files and member_files[0]["file_name"].startswith("my_annual_checkup")
    assert "caregiver@mirobody.ai" in member_files[0]["file_key"], (
        "file_key must be per-member-stable so replays upsert, not duplicate"
    )
    assert blobs, (
        "seeded documents wrote no storage blob — the file page's "
        "'view original' link would be a dead link"
    )
