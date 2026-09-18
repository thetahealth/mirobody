"""Move a deployment's history from the retired `th_series_data` into the
observation model.

`90_retire.sql` renames the old table to
`th_series_data_retired_15` and leaves its rows alone; this pass reads them
in id order and writes them through `pulse.observations`, the same seam
every live writer uses, so a migrated reading is folded, parsed, placed on
its day and coded exactly like a new one. It is idempotent: a re-run skips
what it already wrote (the identity index), and bounded by `batch` so a
large history can be moved over several invocations.

What the old rows lose and what the new ones say about it:

* a file row's name was written in the user's language by the extractor,
  not as printed on the report; the migrated row is marked
  `note_text = migrated:th_series_data` so a reader knows the name is a
  translation and the report is where the original is;
* a file row's unit, reference range and method were JSON inside the
  encrypted `comment`; they land in their own columns. A comment the
  connection's key cannot decrypt is counted as `undecrypted` and the unit
  is then read off the value cell alone, so check `PG_ENCRYPTION_KEY` when
  that count is not zero;
* a soft-deleted row (`deleted = 1`) is not migrated: the person removed it.

Run: `mirobody migrate-observations` (requires the [app] extra and the
deployment's config). Progress and counts are logged; nothing is printed
that names a person or a value.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from mirobody.utils import execute_query

from . import observations

logger = logging.getLogger(__name__)

RETIRED = "th_series_data_retired_15"
MIGRATED_NOTE = "migrated:th_series_data"
#: What `encrypt_content` prefixes; a comment still carrying it after
#: `decrypt_content` was encrypted under another key.
_CIPHER_PREFIX = "gAAAA"

_SELECT = """
SELECT id, user_id, indicator, value, start_time, end_time, source, source_table, source_table_id,
       task_id, decrypt_content(comment) AS comment, fhir_mapping_info, source_class
  FROM """ + RETIRED + """
 WHERE deleted = 0 AND id > :after {user_filter}
 ORDER BY id
 LIMIT :batch
"""


def _legacy_row(r: dict[str, Any]) -> tuple[dict[str, Any], bool]:
    """One retired row to the dict shape `observations.legacy_draft` reads,
    and whether its comment was still ciphertext."""
    row = dict(r)
    comment = row.get("comment") or ""
    undecrypted = isinstance(comment, str) and comment.startswith(_CIPHER_PREFIX)
    if str(row.get("source_table") or "") == "th_files":
        meta: dict[str, Any] = {}
        if isinstance(comment, str) and comment.startswith("{"):
            try:
                meta = json.loads(comment)
            except ValueError:
                meta = {}
        row["unit"] = meta.get("unit") or ""
        row["reference_range"] = meta.get("reference_range") or ""
        row["detection_method"] = meta.get("detection_method") or ""
        row["comment"] = MIGRATED_NOTE
    elif undecrypted:
        row["comment"] = ""
    return row, undecrypted


async def migrate(*, batch: int = 2000, user_id: str | None = None, max_batches: int = 10_000) -> dict[str, Any]:
    """Move rows in id order. Returns the counts: `read`, `written`, `coded`,
    `skipped` (already present), `undecrypted`, `batches` and `rejected`,
    a dict of reason to count."""
    after = 0
    counts: dict[str, Any] = {
        "read": 0, "written": 0, "coded": 0, "skipped": 0, "undecrypted": 0, "batches": 0, "rejected": {},
    }
    user_filter = " AND user_id = :user_id" if user_id else ""
    params: dict[str, Any] = {"batch": batch}
    if user_id:
        params["user_id"] = str(user_id)
    while counts["batches"] < max_batches:
        rows = await execute_query(_SELECT.format(user_filter=user_filter), {**params, "after": after}, log_sql=False) or []
        if not rows:
            break
        counts["batches"] += 1
        counts["read"] += len(rows)
        after = int(rows[-1]["id"])
        legacy = []
        for r in rows:
            row, undecrypted = _legacy_row(r)
            legacy.append(row)
            counts["undecrypted"] += int(undecrypted)
        report = await observations.ingest_legacy(legacy, on_conflict=observations.ON_CONFLICT_SKIP)
        counts["written"] += report.inserted
        counts["coded"] += report.coded
        counts["skipped"] += report.skipped
        for reason, n in report.rejected.items():
            counts["rejected"][reason] = counts["rejected"].get(reason, 0) + n
        logger.info(
            "migrate-observations: batch=%d read=%d written=%d skipped=%d rejected=%d undecrypted=%d last_id=%d",
            counts["batches"], counts["read"], counts["written"], counts["skipped"],
            sum(counts["rejected"].values()), counts["undecrypted"], after,
        )
    return counts


__all__ = ["MIGRATED_NOTE", "RETIRED", "migrate"]
