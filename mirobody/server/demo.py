"""Demo seed: one record per account in the database, the rest through uploads.

A first-time self-hoster signs in and, without this, finds nothing: the agent
works and has nothing to work on. Two accounts fill that gap and stand in a care
circle, `mom@mirobody.ai` sharing their record with `you@mirobody.ai`,
view-only. The same question answers differently
depending on who signed in, and one of the two records is one you can only read.
Any other address in `EMAIL_PREDEFINE_CODES` gets the same record as `you` and a
seat in the same circle, so a deployment that added its own accounts is not
looking at an empty demo.

What the database gets directly is ONE set per account: a year of device
readings and one lab panel. Everything else arrives the way a real record grows,
through the upload page, from `demo/upload/`: a PDF, a phone photo of a printed
slip, a spreadsheet and a CSV, none of them seeded. Uploading one walks the
whole chain rather than being a no-op. ① Collect stores the file, ② Translate
turns its analytes into readings in the catalogue's unit, ③ Agent answers over
both panels at once and charts them against the device series. The seeded panel
prints mg/dL where the uploaded one prints mmol/L, on purpose: what makes the
two one series is the conversion.

Nothing here ships in the wheel. The files live in the repo-root `demo/`
directory, beside `frontend/`, for the same reason that one is there: the
application is `git clone && ./deploy.sh` (`requirements.txt` is `-e .[app]`),
never a `pip install`, so demo data inside the package would be dead weight for
everyone who installs the library. `DEMO_DATA_DIR` overrides the location; a
deployment with neither logs what it looked for and starts anyway.

Dates are literals rather than offsets from today, so a replay upserts the same
rows and a recorded walkthrough keeps matching what a reader sees. The device
window ends `_LAST_DAY`; moving the demo forward is a search and replace here
and in `demo/`.

Deliberately NOT dependent on embeddings. `query_health_indicators` answers from
plain SQL on two of its three paths, the catalogue ("what do I have?") and named
indicators, and only fuzzy keyword search needs pgvector, where it degrades to
the catalogue rather than erroring. So the seeded half works with no LLM or
embedding key configured at all; the upload half needs the one model key.

Off by default; `SEED_DEMO_DATA=true` turns it on and `compose.yaml` sets that
for the Docker path. Every statement is an upsert because the bootstrap replays
on every start.
"""

from __future__ import annotations

import json
import logging
import os
import random
from datetime import date, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)

#: Where the demo files live: `DEMO_DATA_DIR`, else the repo-root `demo/` beside
#: this checkout. `parents[2]` because the application always runs from a source
#: tree (`requirements.txt` is `-e .[app]`); a `pip install` of the library has
#: no demo data and is not meant to.
DEMO_DIR = Path(os.environ.get("DEMO_DATA_DIR") or Path(__file__).resolve().parents[2] / "demo")
SEED_DIR = DEMO_DIR / "seed"

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


#: The last day of the seeded device window, and the day every offset below
#: counts back from.
_LAST_DAY = date(2026, 9, 14)
_SPAN_DAYS = 365

#: The string the end-to-end PHI check greps the running container's logs for.
#: The other two layers are static (`testing.phi_lint` reads the AST of every
#: log statement) and runtime (`ops.PHIPolicy`, a logging filter); this is the
#: only one that tests the system. A string, not a number, because a leaked bare
#: value is indistinguishable from any other number in a log while a leaked
#: comment is unambiguous. Demo data only; never seeded under PRODUCTION.
PHI_CANARY = "self-tracked PHI-CANARY-3f9a"


#: Two people on the same screen. Ask both accounts "how has my HbA1c been" and
#: the answers have to differ, or the isolation is a claim rather than a
#: demonstration. Keyed by the local part of the address; anything else gets
#: `you`, because a deployment's own addresses must still get a record.

#: `weight_kg`, `resting_hr` and `sleep_h` are (first, last) over the year and
#: the series runs between them; `steps` is (median, spread). `sleep_h` is None
#: for one account, so the two records differ in WHICH indicators exist and not
#: only in their values.
_PROFILES: dict[str, dict] = {
    # The address the walkthrough signs in as: healthy, active, labs in range.
    "you": {
        "name": "You (demo)",
        "weight_kg": (70.4, 69.4),
        "resting_hr": (59, 56),
        "steps": (9800, 2400),
        "sleep_h": None,
        "bp": (114, 74, 28),
        "lab": ("2025-11-12", "you_lab_2025-11.md", (
            ("Glycated Hemoglobin-HbA1c", 5.3, "%"),
            ("Fasting Blood Glucose-FBG", 5.1, "mmol/L"),
            ("Total Cholesterol-TC", 4.60, "mmol/L"),
        )),
        "uploads": ("you_annual_checkup_2026-05.pdf", "you_lipid_panel_2026-08.csv"),
    },
    # The person whose record you are following: heavier, less active, sleeping
    # badly, and an
    # HbA1c that has moved into the range a clinician comments on. This is the
    # record shared into the circle, so it is the one a reader reads twice.
    "mom": {
        "name": "Mom (demo)",
        "weight_kg": (82.6, 84.6),
        "resting_hr": (71, 75),
        "steps": (4600, 1500),
        "sleep_h": (6.1, 5.4),
        "bp": (132, 86, 14),
        "lab": ("2025-11-12", "mom_lab_2025-11.md", (
            ("Glycated Hemoglobin-HbA1c", 5.8, "%"),
            ("Fasting Blood Glucose-FBG", 5.6, "mmol/L"),
            ("Total Cholesterol-TC", 5.30, "mmol/L"),
        )),
        "uploads": ("mom_physical_2026-06.jpg", "mom_clinic_visit_2026-07.xlsx"),
    },
}

#: Whose record is shared into the circle, and what everyone else falls back to.
_SHARED_PROFILE = "mom"
_DEFAULT_PROFILE = "you"


def profile_key(email: str) -> str:
    """The profile this address gets. Unknown addresses get the default."""
    local = (email or "").split("@", 1)[0].strip().lower()
    return local if local in _PROFILES else _DEFAULT_PROFILE


def profile_for(email: str) -> dict:
    return _PROFILES[profile_key(email)]


def documents_for(email: str) -> list[dict]:
    """The seeded document this address owns, read from `demo/seed/`.

    One document, not two. The second panel is a real file in `demo/upload/`
    that a reader uploads, which is the only way the demo can show ① Collect
    and ② Translate doing anything.
    """
    name = profile_for(email)["lab"][1]
    path = SEED_DIR / name
    if not path.is_file():
        logger.warning("demo seed: no seeded document on disk (set DEMO_DATA_DIR)")
        return []
    return [{"name": name, "scene": "report", "body": path.read_text(encoding="utf-8")}]


def _between(first: float, last: float, step: int, steps: int) -> float:
    """Where a linear drift from *first* to *last* sits at *step* of *steps*."""
    return first + (last - first) * (step / max(steps - 1, 1))


def _member_series(member_id: str, email: str) -> list[dict]:
    """A year of device readings plus one lab panel, for one account.

    Generated rather than vendored: the fixture this replaced was a 207 KB blob
    whose provenance needed three paragraphs, and a year of daily readings is a
    loop. Seeded per account and per indicator, so a replay writes the same
    values and `update_revive` is an update rather than a second row.
    """
    profile = profile_for(email)
    common = {"user_id": member_id, "source": "demo.self_tracked", "indicator_id": "", "task_id": ""}
    first_day = _LAST_DAY - timedelta(days=_SPAN_DAYS - 1)

    def row(indicator, value, day, unit, comment, time="08:00:00", document=""):
        """`document` is the file this reading was READ OFF, or "" for a device.

        `collect.query._reading_row` takes a reading's `source_table_id` as its
        file key verbatim, so the seed used to hand the UI an email address
        there and "open the source document" answered 404 on every demo row.
        A device reading has no document and must say so with an empty one, or
        the button renders with nothing behind it.
        """
        ts = f"{day} {time}"
        return dict(common, indicator=indicator, value=str(value),
                    start_time=ts, end_time=ts,
                    source_table="th_files" if document else "demo_seed",
                    source_table_id=f"demo/{email}/{document}" if document else "",
                    comment=comment, fhir_mapping_info=json.dumps({"unit": unit}))

    def noise(indicator: str) -> random.Random:
        return random.Random(f"{email}:{indicator}")

    rows: list[dict] = []

    # Weekly weigh-ins, drifting the way the profile says.
    rng = noise("weight")
    mondays = [d for d in (first_day + timedelta(days=i) for i in range(_SPAN_DAYS)) if d.weekday() == 0]
    for step, day in enumerate(mondays):
        kg = _between(*profile["weight_kg"], step, len(mondays)) + rng.uniform(-0.35, 0.35)
        rows.append(row("bodyMasss", round(kg, 1), day, "kg", "Bathroom scale, self-tracked"))

    # Resting heart rate and steps, every day. Steps drop at the weekend for
    # the same reason they do for most people.
    rng = noise("resting_hr")
    for step in range(_SPAN_DAYS):
        day = first_day + timedelta(days=step)
        bpm = _between(*profile["resting_hr"], step, _SPAN_DAYS) + rng.uniform(-2, 2)
        rows.append(row("dailyRestingHeartRates", round(bpm), day, "count/min", "Watch, self-tracked"))

    rng = noise("steps")
    median, spread = profile["steps"]
    for step in range(_SPAN_DAYS):
        day = first_day + timedelta(days=step)
        count = rng.gauss(median, spread) * (0.75 if day.weekday() >= 5 else 1.0)
        rows.append(row("dailySteps", max(round(count), 500), day, "count",
                        "Watch, self-tracked", time="23:59:00"))

    # Home blood pressure, as often as this person actually checks.
    rng = noise("bp")
    systolic, diastolic, every = profile["bp"]
    for step in range(0, _SPAN_DAYS, every):
        day = first_day + timedelta(days=step)
        rows.append(row("systolicPressures", systolic + rng.randint(-5, 5), day, "mmHg",
                        "Home cuff, self-tracked"))
        rows.append(row("diastolicPressures", diastolic + rng.randint(-4, 4), day, "mmHg",
                        "Home cuff, self-tracked"))

    if profile["sleep_h"]:
        rng = noise("sleep")
        for step in range(_SPAN_DAYS):
            day = first_day + timedelta(days=step)
            hours = _between(*profile["sleep_h"], step, _SPAN_DAYS) + rng.uniform(-0.8, 0.8)
            rows.append(row("sleepDuration", round(hours, 1), day, "hours",
                            "Watch, self-tracked", time="07:00:00"))

    # The seeded panel, last, under the names a lab report PRINTS rather than
    # the device catalogue's. Two reasons, both measured: `collect.query`
    # resolves the indicator NAME to a LOINC code at read time, and
    # `GlycatedHemoglobin-HbA1c` does not resolve while `Glycated Hemoglobin-
    # HbA1c` is 4548-4; and the file this account uploads prints the same
    # names, so the upload lands on this series instead of beside it.
    lab_day, document, panel = profile["lab"]
    for indicator, value, unit in panel:
        rows.append(row(indicator, value, lab_day, unit, "Lab draw",
                        time="09:15:00", document=document))
    rows[-1]["comment"] = PHI_CANARY
    return rows


async def _put_blob(file_key: str, text: str) -> None:
    """Store the document's bytes where the row's file_key points.

    The row alone is enough for the agent's VFS (it reads original_text), but
    the file page's "view original" link serves the BLOB at file_key: without
    one the link is dead and every listing logs a "File not found" warning.
    Best-effort: a storage failure must not fail the seed.
    """
    try:
        from mirobody.utils.config.storage.factory import get_storage_client
        _, err = await get_storage_client().put(
            key=file_key,
            content=text.encode("utf-8"),
            content_type="text/markdown",
        )
        if err:
            logger.warning("demo seed: could not store a blob: error_type=%s", type(err).__name__)
    except Exception as e:
        logger.warning("demo seed: could not store a blob: error_type=%s", type(e).__name__)


async def _seed_account(execute_query, member_id: str, email: str) -> int:
    """Give one account its year of readings and its one document."""
    # Imported here, not at module top: the sibling `_member_series` test runs
    # on a bare `pip install mirobody`, and `collect.observations` pulls in `utils`.
    from mirobody.collect import observations

    rows = _member_series(member_id, email)
    written = await observations.ingest_legacy_rows(rows)

    # The file key here is the one `_member_series` wrote into the lab rows'
    # `source_table_id`, so a reading opens the document it was read off.
    lab_day, _, panel = profile_for(email)["lab"]
    for document in documents_for(email):
        body = document["body"]
        file_key = f"demo/{email}/{document['name']}"
        await execute_query(
            _UPSERT_FILE,
            {
                "user_id": member_id,
                "file_name": document["name"],
                "file_key": file_key,
                # The file-list shaper and the agent's file listing read all of
                # this out of one JSON blob. A bare '{}' rendered as "0 B",
                # "0 indicators" and an empty report date in the UI, over a
                # file three readings point at. `extracted` is the honest
                # source: the date is printed on the document.
                "file_content": json.dumps({
                    "file_size": len(body.encode("utf-8")),
                    "indicators_count": len(panel),
                    "report_date": f"{lab_day} 09:15:00",
                    "date_source": "extracted",
                    "date_confirmed": True,
                    "processed": True,
                }),
                "scene": document["scene"],
                "created_source_id": email,
                "original_text": body,
                "text_length": len(body),
            },
            log_sql=False,
        )
        await _put_blob(file_key, body)
    return written


async def _account_id(execute_query, email: str, name: str) -> str:
    row = await execute_query(
        _UPSERT_USER, {"email": email, "name": name, "tz": "UTC"}, log_sql=False
    )
    if row:
        return str(row[0]["id"])
    # ON CONFLICT ... DO UPDATE returns the row, but a concurrent insert can
    # leave RETURNING empty; read it back rather than abandoning the seed.
    row = await execute_query(_SELECT_USER, {"email": email}, log_sql=False)
    return str(row[0]["id"]) if row else ""


async def seed(member_emails: list[str]) -> None:
    """Seed each address in *member_emails* and put them in one care circle.

    *member_emails* are the accounts a human will actually sign in as: the keys
    of `EMAIL_PREDEFINE_CODES`. Each is created if absent, because the circle
    needs a member id and `add_or_get_user` only runs on first login: without
    this, the first sign-in would land on an account that is in no circle.
    """
    from mirobody.user import care_circle as cc
    from mirobody.utils import execute_query

    accounts: list[tuple[str, str]] = []
    reading_count = 0
    for email in member_emails:
        member_id = await _account_id(execute_query, email, profile_for(email)["name"])
        if not member_id:
            continue
        reading_count += await _seed_account(execute_query, member_id, email)
        accounts.append((email, member_id))

    # Whose record gets shared: `mom`'s if that account exists, else the first
    # address, so a deployment with its own two accounts still has a circle to
    # look at.
    shared = next((a for a in accounts if profile_key(a[0]) == _SHARED_PROFILE), None) or (
        accounts[0] if accounts else None
    )
    viewer_count = 0
    if shared:
        _, owner_id = shared
        circle_id = await cc.ensure_own_circle(owner_id, name="Demo care circle")
        # `health_access` is set on the OWNER's row, because that is where the
        # switch lives: it says what this person shares with the circle, and it
        # is the only reason anyone else can read it. VIEW, not EDIT: a
        # walkthrough should not be able to edit the record it is reading.
        await cc.set_health_access(owner_id, circle_id, cc.ACCESS_VIEW)
        for email, member_id in accounts:
            if member_id == owner_id:
                continue
            # The real invite/accept pair, run on the member's behalf, rather
            # than a shortcut: these are accounts a human signs in as, so their
            # own `health_access` must stay at 0. The managed-member path would
            # set it to read-write and put the walkthrough in a state the
            # product says is impossible: off by default, each member decides.
            await cc.invite(circle_id, int(member_id))
            await cc.respond_to_invitation(int(member_id), circle_id, accept=True)
            viewer_count += 1

    upload_count = sum(len(p["uploads"]) for p in _PROFILES.values())
    logger.info(
        "demo seed: %d account(s), %d readings, one document each; one record is "
        "shared view-only with %d of them, and demo/upload/ holds %d file(s) the "
        "seed deliberately leaves out",
        len(accounts),
        reading_count,
        viewer_count,
        upload_count,
    )
