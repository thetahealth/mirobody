"""Care-circle demo seed — so a fresh deployment is not an empty database.

A first-time self-hoster signs in and, today, finds nothing: the agent works and
has nothing to work on. This module fills that gap with one synthetic person's
two-year record and shares it into the care circle of EVERY address in
`EMAIL_PREDEFINE_CODES` — not just the one this repo ships. A deployment that
added its own demo accounts, or `./deploy.sh` with a different set, gets the same
data behind whichever of them a human actually signs in as; guessing one name
here would have made the demo look empty for the rest.

It demonstrates two things at once — what the engine does with real volume, and
what it feels like to hold someone else's record rather than your own. You have
no data; you can still ask about hers and get an answer.

The data is NOT generated here. `care_circle_demo.json.gz` was produced once
from ESL-Bench (`healthmemoryarena/ESL-Bench`) via the sibling
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

_FIXTURE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "care_circle_demo.json.gz")

#: Batch size for the readings insert. `execute_query` with a list of dicts goes
#: through SQLAlchemy executemany; 1000 keeps the round trips down without
#: building a multi-megabyte statement.
_BATCH = 1000

_UPSERT_USER = """
INSERT INTO health_app_user (is_del, email, name, tz)
VALUES (FALSE, :email, :name, :tz)
ON CONFLICT (email) DO UPDATE SET is_del = FALSE
RETURNING id
"""

_SELECT_USER = "SELECT id FROM health_app_user WHERE email = :email AND is_del = FALSE LIMIT 1"

_UPSERT_SERIES = """
INSERT INTO th_series_data (
    user_id, indicator, value, start_time, end_time, source_table,
    source_table_id, comment, indicator_id, source, task_id,
    fhir_mapping_info, create_time, update_time, deleted
) VALUES (
    :user_id, :indicator, :value, :start_time, :end_time, :source_table,
    :source_table_id, :comment, :indicator_id, :source, :task_id,
    :fhir_mapping_info, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 0
)
ON CONFLICT (user_id, indicator, start_time, end_time) DO UPDATE SET
    value       = EXCLUDED.value,
    deleted     = 0,
    update_time = CURRENT_TIMESTAMP
"""

_UPSERT_FILE = """
INSERT INTO th_files (
    user_id, query_user_id, file_name, file_type, file_key, file_content,
    scene, created_source, created_source_id, original_text, text_length,
    is_del, created_at, updated_at
) VALUES (
    :user_id, :user_id, :file_name, 'text/markdown', :file_key, '{}',
    :scene, 'demo_seed', :created_source_id, :original_text, :text_length,
    false, now(), now()
)
ON CONFLICT (file_key) DO UPDATE SET
    original_text = EXCLUDED.original_text,
    text_length   = EXCLUDED.text_length,
    is_del        = false,
    updated_at    = now()
"""

# `status` must be exactly 'authorized': that is what `get_query_user_id`
# (utils/permissions.py) filters on. The column's default is 'pending', which
# would leave the circle visible in listings but refuse every proxied read.
_UPSERT_SHARE = """
INSERT INTO th_share_relationship (
    owner_user_id, member_user_id, owner_email, member_email, status, permissions
) VALUES (
    :owner_id, :member_id, :owner_email, :member_email, 'authorized', '{"all": 1}'::jsonb
)
ON CONFLICT (owner_user_id, member_user_id) DO UPDATE SET
    status     = 'authorized',
    updated_at = CURRENT_TIMESTAMP
"""


def enabled() -> bool:
    return (os.environ.get("SEED_DEMO_DATA") or "").strip().lower() in ("1", "true", "yes", "on")


async def seed(member_emails: list[str]) -> None:
    """Load the fixture and share it with each address in *member_emails*.

    *member_emails* are the accounts a human will actually sign in as — the keys
    of `EMAIL_PREDEFINE_CODES`. Each is created if absent, because the circle
    needs a member id and `add_or_get_user` only runs on first login: without
    this, the first sign-in would land on an account that is in no circle.
    """
    from ..utils import execute_query

    if not os.path.isfile(_FIXTURE):
        logging.warning("demo seed skipped: %s is missing", _FIXTURE)
        return

    with gzip.open(_FIXTURE, "rt", encoding="utf-8") as fh:
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
        logging.error("demo seed aborted: could not resolve an id for %s", owner["email"])
        return

    series = fixture.get("series") or []
    batch: list[dict] = []
    written = 0
    for r in series:
        r = dict(r, user_id=owner_id)
        batch.append(r)
        if len(batch) >= _BATCH:
            await execute_query(_UPSERT_SERIES, batch, log_sql=False)
            written += len(batch)
            batch.clear()
    if batch:
        await execute_query(_UPSERT_SERIES, batch, log_sql=False)
        written += len(batch)

    for doc in fixture.get("documents") or []:
        body = doc["body"]
        await execute_query(
            _UPSERT_FILE,
            {
                "user_id": owner_id,
                "file_name": doc["name"],
                # Stable, so a replay updates the same row instead of adding one.
                "file_key": f"demo/{owner['email']}/{doc['name']}",
                "scene": doc.get("scene") or "others",
                "created_source_id": owner["email"],
                "original_text": body,
                "text_length": len(body),
            },
            log_sql=False,
        )

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
        await execute_query(
            _UPSERT_SHARE,
            {
                "owner_id": owner_id,
                "member_id": member_id,
                "owner_email": owner["email"],
                "member_email": email,
            },
            log_sql=False,
        )
        shared.append(email)

    logging.info(
        "demo seed: %s → %d readings, %d documents; care circle shared with %s. %s",
        owner["email"], written, len(fixture.get("documents") or []),
        ", ".join(shared) or "(nobody)", fixture.get("note", ""),
    )
