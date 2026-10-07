"""Move a deployment's history from the retired `th_series_data` into the
observation model.

`90_retire.sql` renames the old table to `th_series_data_retired_15` and
leaves its rows alone; this pass reads them in id order and writes them
through `collect.observations`, the same seam every live writer uses, so a
migrated reading is folded, parsed, placed on its day and coded exactly like
a new one. It is bounded by `batch`, so a large history can be moved over
several invocations, and a re-run starts where the last one left rows.

A row is marked moved (`deleted = 2`) only once a row with the same
fingerprint is in the observation model: written now, or found there from an
earlier run. Anything else stays live, and the table is dropped (here, or by
the next boot's `90_retire.sql`) only when no live row is left:

* `differs`: the identity already holds a different row. Before 1.5.4 a
  re-run counted that as present, so a row written from a comment that did
  not decrypt (no unit, range or method) passed for the full one, and
  dropping the table would have lost the original. `repair=True` amends such
  a row when nobody has changed it since it was written;
* `undecrypted`: the comment does not decrypt under this connection's key.
  The row is not written, so the right key can still bring it over whole;
  `write_undecrypted=True` writes it without what the comment held, for a
  key that is lost for good;
* `rejected`: the new model cannot hold the row; it goes only by hand.

`verify_only=True` writes nothing: it marks the rows already present and
counts the rest as `missing`. That is the safe first run for a database
migrated before 1.5.4, where a reading the person erased since is missing
too, and nothing recorded the erasure to tell the two apart.

What the old rows lose and what the new ones say about it:

* a file row's name was written in the user's language by the extractor,
  not as printed on the report; the migrated row is marked
  `note_text = migrated:th_series_data` so a reader knows the name is a
  translation and the report is where the original is;
* a file row's unit, reference range and method were JSON inside the
  encrypted `comment`; they land in their own columns;
* a soft-deleted row (`deleted = 1`) is not migrated: the person removed it,
  and it goes with the table.

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

RETIRED = observations.RETIRED_READINGS
MIGRATED_NOTE = "migrated:th_series_data"
#: `deleted` of a row whose copy is in the observation model.
MOVED = 2
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


async def migrate(
    *,
    batch: int = 2000,
    user_id: str | None = None,
    max_batches: int = 10_000,
    repair: bool = False,
    write_undecrypted: bool = False,
    verify_only: bool = False,
) -> dict[str, Any]:
    """Move rows in id order. Returns the counts: `read`, `written`, `coded`,
    `skipped` (an equal row was already there), `differs`, `missing`
    (under `verify_only`: not there, and not written), `undecrypted`,
    `batches`, `rejected` (a dict of reason to count), `present` (whether
    there was a retired table at all), `left` (live rows after a pass that
    reached the end; None otherwise) and `dropped`."""
    after = 0
    counts: dict[str, Any] = {
        "read": 0, "written": 0, "coded": 0, "skipped": 0, "differs": 0, "missing": 0, "undecrypted": 0, "batches": 0,
        "rejected": {}, "present": False, "left": None, "dropped": False,
    }
    found = await execute_query("SELECT to_regclass(:name) IS NOT NULL AS present", {"name": RETIRED}, log_sql=False)
    if not (found and found[0]["present"]):
        return counts
    counts["present"] = True
    finished = False
    user_filter = " AND user_id = :user_id" if user_id else ""
    params: dict[str, Any] = {"batch": batch}
    if user_id:
        params["user_id"] = str(user_id)
    on_conflict = observations.ON_CONFLICT_REPAIR if repair else observations.ON_CONFLICT_VERIFY
    while counts["batches"] < max_batches:
        rows = await execute_query(_SELECT.format(user_filter=user_filter), {**params, "after": after}, log_sql=False) or []
        if not rows:
            finished = True
            break
        counts["batches"] += 1
        counts["read"] += len(rows)
        after = int(rows[-1]["id"])
        legacy, ids = [], []
        for r in rows:
            row, undecrypted = _legacy_row(r)
            counts["undecrypted"] += int(undecrypted)
            if undecrypted and not write_undecrypted:
                continue
            legacy.append(row)
            ids.append(int(r["id"]))
        if verify_only:
            present = await observations.legacy_present(legacy) if legacy else []
            moved = [i for i, here in zip(ids, present, strict=True) if here]
            counts["skipped"] += len(moved)
            counts["missing"] += len(ids) - len(moved)
            report = observations.Report()
        else:
            report = await observations.ingest_legacy(legacy, on_conflict=on_conflict) if legacy else observations.Report()
            moved = [i for i, outcome in zip(ids, report.outcomes, strict=True)
                     if outcome in (observations.OUTCOME_INSERTED, observations.OUTCOME_SKIPPED)]
        if moved:
            await execute_query(f"UPDATE {RETIRED} SET deleted = {MOVED} WHERE id = ANY(:ids)", {"ids": moved}, log_sql=False)
        counts["written"] += report.inserted
        counts["coded"] += report.coded
        counts["skipped"] += report.skipped
        counts["differs"] += report.differs
        for reason, n in report.rejected.items():
            counts["rejected"][reason] = counts["rejected"].get(reason, 0) + n
        batches, read_count, written, skipped = counts["batches"], counts["read"], counts["written"], counts["skipped"]
        differs_count, missing_count, undecrypted = counts["differs"], counts["missing"], counts["undecrypted"]
        rejected_count, last_id = sum(counts["rejected"].values()), after
        logger.info(
            "migrate-observations: batch=%d read=%d written=%d skipped=%d differs=%d missing=%d rejected=%d "
            "undecrypted=%d last_id=%d",
            batches, read_count, written, skipped, differs_count, missing_count, rejected_count, undecrypted, last_id,
        )
    if finished:
        left = await execute_query(f"SELECT count(*) AS n FROM {RETIRED} WHERE deleted = 0", {}, log_sql=False)
        counts["left"] = int(left[0]["n"]) if left else None
        if counts["left"] == 0:
            await execute_query(f"DROP TABLE {RETIRED}", {}, log_sql=False)
            counts["dropped"] = True
            logger.info("migrate-observations: every row moved, %s dropped", RETIRED)  # phi: ok a table name constant
    return counts


__all__ = ["MIGRATED_NOTE", "MOVED", "RETIRED", "migrate"]
