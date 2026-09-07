"""Care-circle demo seed — so a fresh deployment is not an empty database.

A first-time self-hoster signs in and, today, finds nothing: the agent works and
has nothing to work on. This module fills that gap with one synthetic person's
two-year record and shares it into the care circle of EVERY address in
`EMAIL_PREDEFINE_CODES` — not just the one this repo ships. A deployment that
added its own demo accounts, or `./deploy.sh` with a different set, gets the same
data behind whichever of them a human actually signs in as; guessing one name
here would have made the demo look empty for the rest.

It demonstrates two things at once — what the engine does with real volume, and
what it feels like to hold someone else's record next to your own. Each sign-in
account gets a THIN record of its own (a few weeks of self-tracked vitals and
one unremarkable annual checkup, ~two dozen readings) beside the synthetic
person's two-year, 244-indicator record shared into the circle. That contrast
is the point: ask about YOUR HbA1c and you get one normal value from your own
data; ask about HERS and the answer comes from a record you merely have view
access to — data isolation you can see, not just read about.

The data is NOT generated here and does NOT ship in the wheel. It lives in
the repo-root `demo/` directory, beside `frontend/`, for the reason that one
is there too: the application is `git clone && ./deploy.sh` (`requirements.txt`
is `-e .[app]`), never a `pip install`, so a 230 KB fixture inside the package
would only be dead weight for the far larger number of people who install the
LIBRARY. `DEMO_DATA_DIR` overrides the location; a deployment with neither
logs what it looked for and starts anyway.

`care_circle_demo.json.gz` was produced once from ESL-Bench
(`healthmemoryarena/ESL-Bench`) via the sibling
[mirobody-eval](https://github.com/thetahealth/mirobody-eval) and vendored, for
three reasons: container startup has no business downloading 20 MB from
HuggingFace, `mirobody-eval` needs an embedding key this repo does not require,
and a demo that fails when the network is down is worse than no demo.

Deliberately NOT dependent on embeddings. `query_health_indicators` answers from
plain SQL on two of its three paths — the catalogue ("what does she have?") and
named indicators — and only fuzzy keyword search needs pgvector, where it
degrades to the catalogue rather than erroring. So this works with no LLM or
embedding key configured at all.

Off by default; `SEED_DEMO_DATA=true` turns it on and `compose.yaml` sets that
for the Docker path. Every statement is an upsert because the bootstrap replays
on every start.
"""

from __future__ import annotations

import gzip
import json
import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)

#: Where the fixture lives: `DEMO_DATA_DIR`, else the repo-root `demo/` beside
#: this checkout. `parents[2]` because the application always runs from a
#: source tree (`requirements.txt` is `-e .[app]`); a `pip install` of the
#: library has no demo data and is not meant to.
DEMO_DIR = Path(os.environ.get("DEMO_DATA_DIR") or Path(__file__).resolve().parents[2] / "demo")
FIXTURE = DEMO_DIR / "care_circle_demo.json.gz"

_UPSERT_USER = """
INSERT INTO health_app_user (is_del, email, name, tz)
VALUES (FALSE, :email, :name, :tz)
ON CONFLICT (email) DO UPDATE SET is_del = FALSE
RETURNING id
"""

_SELECT_USER = "SELECT id FROM health_app_user WHERE email = :email AND is_del = FALSE LIMIT 1"

_UPSERT_FILE = """
INSERT INTO th_files (
    user_id, query_user_id, file_name, file_type, file_key, file_content,
    scene, created_source, created_source_id, original_text, text_length,
    is_del, created_at, updated_at
) VALUES (
    :user_id, :user_id, :file_name, 'text/markdown', :file_key, :file_content,
    :scene, 'web_drive', :created_source_id, :original_text, :text_length,
    false, now(), now()
)
ON CONFLICT (file_key) DO UPDATE SET
    original_text = EXCLUDED.original_text,
    text_length   = EXCLUDED.text_length,
    is_del        = false,
    updated_at    = now()
"""



def enabled() -> bool:
    return (os.environ.get("SEED_DEMO_DATA") or "").strip().lower() in ("1", "true", "yes", "on")


# ── The sign-in account's OWN record ─────────────────────────────────
#
# A thin, healthy, self-tracked slice — deliberately the opposite of the
# synthetic person's 244-indicator clinical record, so the walkthrough can
# SHOW isolation instead of asserting it: the same question ("my HbA1c?" /
# "her HbA1c?") answers from two different records with two different
# stories. All values are ordinary-normal; dates are fixed so replays upsert
# the same rows.

#: The string the end-to-end PHI check greps the running container's logs for.
#:
#: The discipline has a static layer (`testing.phi_lint`, which reads the AST of
#: every log statement) and a runtime one (`ops.PHIPolicy`, a logging filter).
#: This is the third, and the only one that tests the SYSTEM: a redaction that
#: holds in unit tests and not in the container is not a redaction.
#:
#: It is a STRING, not an odd number, on purpose. A leaked bare value is
#: indistinguishable from any other number in a log; a leaked comment is
#: unambiguous, and a comment is free-text health data — the thing the column
#: encryption at rest exists to protect. It rides on an existing row's comment
#: rather than on a row of its own, so the demo's charts are unchanged.
#:
#: Demo data only, and the demo never seeds under PRODUCTION.
PHI_CANARY = "self-tracked PHI-CANARY-3f9a"

#: Which row from the end carries it. The last one is the storyline's HbA1c and
#: its comment is read by a walkthrough, so the canary sits one before.
_CANARY_ROW = 3


def _member_series(member_id: str, email: str) -> list[dict]:
    common = {
        "user_id": member_id,
        "source": "demo.self_tracked",
        "source_table": "demo_seed",
        "source_table_id": email,
        "indicator_id": "",
        "task_id": "",
    }

    def row(indicator, value, day, unit, comment, time="08:00:00"):
        ts = f"{day} {time}"
        return dict(common, indicator=indicator, value=str(value),
                    start_time=ts, end_time=ts,
                    comment=comment, fhir_mapping_info=json.dumps({"unit": unit}))

    rows: list[dict] = []
    # A month of Monday weigh-ins.
    for day, kg in (("2025-03-03", 70.4), ("2025-03-10", 70.1), ("2025-03-17", 70.2),
                    ("2025-03-24", 69.8), ("2025-03-31", 69.9), ("2025-04-07", 69.6)):
        rows.append(row("bodyMasss", kg, day, "kg", "Bathroom scale, self-tracked"))
    # Resting heart rate, same mornings.
    for day, bpm in (("2025-03-03", 58), ("2025-03-10", 57), ("2025-03-17", 59),
                     ("2025-03-24", 56), ("2025-03-31", 57)):
        rows.append(row("dailyRestingHeartRates", bpm, day, "count/min", "Watch, self-tracked"))
    # A week of steps.
    for day, steps in (("2025-03-24", 9412), ("2025-03-25", 11250), ("2025-03-26", 8103),
                       ("2025-03-27", 10877), ("2025-03-28", 7642), ("2025-03-29", 12490),
                       ("2025-03-30", 6889)):
        rows.append(row("dailySteps", steps, day, "count", "Watch, self-tracked", time="23:59:00"))
    # Two home blood-pressure checks.
    for day, sys_v, dia_v in (("2025-03-10", 114, 74), ("2025-04-07", 118, 76)):
        rows.append(row("systolicPressures", sys_v, day, "mmHg", "Home cuff, self-tracked"))
        rows.append(row("diastolicPressures", dia_v, day, "mmHg", "Home cuff, self-tracked"))
    # The one lab value that makes the isolation contrast land: the SAME
    # indicator the shared record's storyline turns on (hers: 7.2→6.5→6.6),
    # here boring and normal.
    for day, pct in (("2024-11-12", 5.3), ("2025-05-06", 5.2)):
        rows.append(row("GlycatedHemoglobin-HbA1c", pct, day, "%",
                        "Annual checkup lab draw", time="09:15:00"))
    # The PHI sentinel rides on one of these rows — see PHI_CANARY.
    rows[-_CANARY_ROW]["comment"] = PHI_CANARY
    return rows


_MEMBER_DOCUMENT = {
    "name": "my_annual_checkup_2025-05.md",
    "scene": "report",
    "body": """# Annual checkup — 2025-05-06

Routine annual physical. Everything within reference ranges.

| Test | Result | Reference |
| --- | --- | --- |
| HbA1c | 5.2 % | < 5.7 % |
| Fasting glucose | 88 mg/dL | 70–99 mg/dL |
| Total cholesterol | 172 mg/dL | < 200 mg/dL |
| LDL cholesterol | 96 mg/dL | < 130 mg/dL |
| HDL cholesterol | 58 mg/dL | > 40 mg/dL |
| Triglycerides | 84 mg/dL | < 150 mg/dL |
| Blood pressure | 116/75 mmHg | < 120/80 mmHg |
| Resting heart rate | 57 bpm | 60–100 bpm (athletic: lower) |

Physician note: no findings. Continue current activity level; next routine
checkup in 12 months.

*This is the sign-in account's own record — synthetic, like everything the
demo seeds. The two-year record with the HbA1c story belongs to the person
sharing with you, not to you.*
""",
}


async def _put_blob(file_key: str, text: str) -> None:
    """Store the document's bytes where the row's file_key points.

    The row alone is enough for the agent's VFS (it reads original_text), but
    the file page's "view original" link serves the BLOB at file_key — without
    one the link is dead and every listing logs a "File not found" warning.
    Best-effort: a storage failure must not fail the seed.
    """
    try:
        from ..utils.config.storage.factory import get_storage_client
        _, err = await get_storage_client().put(
            key=file_key,
            content=text.encode("utf-8"),
            content_type="text/markdown",
        )
        if err:
            logger.warning("demo seed: could not store a blob: error_type=%s", type(err).__name__)
    except Exception as e:
        logger.warning("demo seed: could not store a blob: error_type=%s", type(e).__name__)


async def _seed_member_own_data(execute_query, member_id: str, email: str) -> int:
    """Give a sign-in account its own thin record. Returns readings written."""
    # Imported here, not at module top: the sibling `_member_series` test runs
    # on a bare `pip install mirobody`, and `pulse.readings` pulls in `utils`.
    from ..pulse.readings import upsert_readings

    rows = _member_series(member_id, email)
    await upsert_readings(rows, on_conflict="update_revive")

    body = _MEMBER_DOCUMENT["body"]
    member_file_key = f"demo/{email}/{_MEMBER_DOCUMENT['name']}"
    await execute_query(
        _UPSERT_FILE,
        {
            "user_id": member_id,
            "file_name": _MEMBER_DOCUMENT["name"],
            "file_key": member_file_key,
            # file_size is read from this JSON blob by the file-list shaper;
            # a bare '{}' rendered as "0 B" in the UI.
            "file_content": json.dumps({"file_size": len(body.encode("utf-8")), "processed": True}),
            "scene": _MEMBER_DOCUMENT["scene"],
            "created_source_id": email,
            "original_text": body,
            "text_length": len(body),
        },
        log_sql=False,
    )
    await _put_blob(member_file_key, body)
    return len(rows)


async def seed(member_emails: list[str]) -> None:
    """Load the fixture and share it with each address in *member_emails*.

    *member_emails* are the accounts a human will actually sign in as — the keys
    of `EMAIL_PREDEFINE_CODES`. Each is created if absent, because the circle
    needs a member id and `add_or_get_user` only runs on first login: without
    this, the first sign-in would land on an account that is in no circle.
    """
    from ..user import care_circle as cc
    from ..utils import execute_query

    if not FIXTURE.is_file():
        # A pip install of the library, or a checkout without the fixture:
        # say where it was looked for rather than starting an empty demo.
        logger.warning("demo seed skipped: no care-circle fixture on disk (set DEMO_DATA_DIR)")
        return

    with gzip.open(FIXTURE, "rt", encoding="utf-8") as fh:
        fixture = json.load(fh)

    owner = fixture["owner"]
    row = await execute_query(
        _UPSERT_USER,
        {"email": owner["email"], "name": owner["name"], "tz": owner.get("tz") or "UTC"},
        log_sql=False,
    )
    owner_id = str(row[0]["id"]) if row else None
    if not owner_id:
        # ON CONFLICT ... DO UPDATE returns the row, but a concurrent insert can
        # leave RETURNING empty; read it back rather than abandoning the seed.
        row = await execute_query(_SELECT_USER, {"email": owner["email"]}, log_sql=False)
        owner_id = str(row[0]["id"]) if row else ""
    if not owner_id:
        logger.error("demo seed aborted: the owner account has no id")
        return

    # A replay must bring the shared record back whatever a walkthrough did to
    # it — hence "update_revive", not the device-sync "update".
    from ..pulse.readings import upsert_readings

    series = fixture.get("series") or []
    written_count = await upsert_readings([dict(r, user_id=owner_id) for r in series], on_conflict="update_revive")

    for doc in fixture.get("documents") or []:
        body = doc["body"]
        owner_file_key = f"demo/{owner['email']}/{doc['name']}"
        await execute_query(
            _UPSERT_FILE,
            {
                "user_id": owner_id,
                "file_name": doc["name"],
                # Stable, so a replay updates the same row instead of adding one.
                "file_key": owner_file_key,
                "file_content": json.dumps({"file_size": len(body.encode("utf-8")), "processed": True}),
                "scene": doc.get("scene") or "others",
                "created_source_id": owner["email"],
                "original_text": body,
                "text_length": len(body),
            },
            log_sql=False,
        )
        await _put_blob(owner_file_key, body)

    shared = []
    for email in member_emails:
        row = await execute_query(
            _UPSERT_USER, {"email": email, "name": email.split("@")[0], "tz": "UTC"}, log_sql=False
        )
        member_id = str(row[0]["id"]) if row else ""
        if not member_id:
            row = await execute_query(_SELECT_USER, {"email": email}, log_sql=False)
            member_id = str(row[0]["id"]) if row else ""
        if not member_id or member_id == owner_id:
            continue
        # The demo person's circle, with each sign-in account accepted into it.
        # `health_access` is set on the OWNER's row, because that is where the
        # switch lives: it says what the synthetic person shares with the
        # circle, and it is the only reason the caregiver can read anything.
        # ACCESS_VIEW, not EDIT — a walkthrough should not be able to edit the
        # record it is reading.
        circle_id = await cc.ensure_own_circle(owner_id, name="Demo care circle")
        await cc.set_health_access(owner_id, circle_id, cc.ACCESS_VIEW)

        # The real invite/accept pair, run on the member's behalf, rather than a
        # shortcut: these are accounts a human signs in as, so their own
        # `health_access` must stay at 0. Seeding them with the managed-member
        # path would set it to read-write and put the walkthrough in a state the
        # product says is impossible — "off by default, each member controls
        # their own".
        await cc.invite(circle_id, int(member_id))
        await cc.respond_to_invitation(int(member_id), circle_id, accept=True)

        # The member's own thin record — see _member_series for why.
        await _seed_member_own_data(execute_query, member_id, email)
        shared.append(email)

    logger.info(
        "demo seed: %d readings, %d documents; care circle shared with %d member(s) "
        "(each also gets their own thin record: ~%d readings + 1 checkup document)",
        written_count,
        len(fixture.get("documents") or []),
        len(shared),
        len(_member_series("0", "x@x")),
    )
