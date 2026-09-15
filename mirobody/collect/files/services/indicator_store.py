"""Readings extracted from a file, written to the observation model.

The write side of what ① Collect hands on: `save_indicators_to_db` takes what
the extractor found in one document and lands it through
`collect.observations.ingest`, the single writer.

Splitting a unit off the value used to happen here, on the string. It happens
in `translate.parse.parse_value` now, which reads the value column and the
unit column together and returns a typed value: the number, the comparator
("<2.5" is 2.5 and "<", not the text), and the unit in UCUM. The extraction
prompt asks for the two columns separately; this is what holds when a model
prints the unit twice anyway.
"""

import logging
from datetime import datetime
from typing import Any

from mirobody.collect import observations

logger = logging.getLogger(__name__)


def generate_source_table_id(msg_id: str, file_key: str) -> str:
    """
    Generate the source id of a file's observations based on file_key.

    Uses file_key directly as source_table_id since source_table is th_files.
    file_key is the unique identifier in th_files table.

    Args:
        msg_id: Message ID (legacy parameter, kept for backward compatibility)
        file_key: File key from th_files table (primary identifier)

    Returns:
        str: file_key as source_table_id, or msg_id as fallback
    """
    # Use file_key directly as source_table_id
    if file_key:
        return file_key

    # Fallback to msg_id if no file_key (legacy support)
    return msg_id or ""

async def save_indicators_to_db(
    user_id: str,
    indicators: list[dict[str, Any]],
    start_time: datetime,
    date_source: str,
    msg_id: str,
    comment: str = "",
    source_table: str = "th_files",
    file_key: str = None,
    extractor: str = "",
) -> int:
    """Write a file's extracted indicators as observations.

    `start_time` and `date_source` come from `resolve_report_date`; the
    caller resolves them so the same answer can be recorded on the file
    row. The extraction is frozen as it was read (`th_extraction`), every
    text field is stored as printed, and the coding sits beside each row.
    A re-upload of the same file is a replay: equal rows are skipped.
    """
    try:
        drafts = []
        for ix, indicator in enumerate(indicators):
            original_indicator = indicator.get("original_indicator")
            if not original_indicator:
                continue
            method = str(indicator.get("detection_method") or "")
            kind = observations.KIND_FINDING if method in ("Imaging", "Pathological") else observations.KIND_MEASUREMENT
            drafts.append(observations.Draft(
                name_text=str(original_indicator),
                observed_start=start_time,
                value_text=str(indicator.get("value") or ""),
                unit_text=str(indicator.get("unit") or ""),
                ref_text=str(indicator.get("reference_range") or ""),
                flag_text=str(indicator.get("status") or ""),
                method_text=method,
                note_text=str(indicator.get("notes") or ""),
                kind=kind,
                row_ix=ix,
            ))

        source_table_id = generate_source_table_id(msg_id, file_key)
        provenance = observations.Provenance(
            modality=observations.MODALITY_LAB,
            source_kind=observations.SOURCE_FILE,
            source_ref=f"{source_table}:{source_table_id}",
            extractor=extractor or "llm:file-parser@indicators-v1",
            report_date=start_time.date() if isinstance(start_time, datetime) else None,
            date_source=date_source,
        )
        tz = await observations.user_tz(str(user_id))
        report = await observations.ingest(str(user_id), drafts, provenance, user_tz=tz, payload=indicators)
        logger.info(
            f"Write complete: inserted={report.inserted} skipped={report.skipped} "
            f"rejected={report.rejected or {}}, user_id: {user_id}"
        )

        if report.inserted:
            # Refresh the user profile. Coalescing + self-guarded, so a
            # Redis hiccup never fails the ingest write above.
            try:
                from mirobody.task import ProfileRefreshTask
                await ProfileRefreshTask.enqueue(str(user_id))
            except Exception as e:
                logger.warning(f"Failed to enqueue profile-refresh signal: {e}")

        return report.inserted + report.skipped

    except Exception:
        logger.error(f"Failed to save indicators to database, user_id: {user_id}", stack_info=True)
        return 0
