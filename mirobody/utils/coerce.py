"""Values that arrive as text and must not raise.

A row read back from the database, a date typed on a lab report, a JSON column
written by an older release: each arrives as whatever the writer felt like, and
a parse failure here is not worth an exception, because the caller has a
sensible default and the alternative is losing the whole record over one field.

These lived in `collect/files/services/db_utils.py`, which was neither
about the database nor about files: three modules outside that package imported
it as a general toolbox, which is what a misnamed module invites.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

#: Tried in order. Both separators, with and without seconds, because a report
#: date is typed by a person and an export date is written by a machine. The
#: printed shapes too (年月日, dots): a small local model copies the date as the
#: report prints it, and MiniCPM5-2B wrote 2026年05月01日 for an XLSX, which
#: then went under the upload day (benchmarks/local_models, 2026-10-06). An
#: app screenshot printing 20260418 went the same way, with either model.
DATE_FORMATS = (
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d",
    "%Y/%m/%d",
    "%Y-%m-%d %H:%M",
    "%Y/%m/%d %H:%M:%S",
    "%Y/%m/%d %H:%M",
    "%Y年%m月%d日",
    "%Y年%m月%d日 %H:%M",
    "%Y年%m月%d日 %H:%M:%S",
    "%Y.%m.%d",
    "%Y.%m.%d %H:%M",
    "%Y%m%d",
)


def safe_json_dumps(data: Any, default: str = "{}") -> str:
    """JSON text for `data`; `default` if it will not serialize.

    A string passes through untouched: the column often already holds JSON.
    """
    if data is None:
        return default
    if isinstance(data, str):
        return data
    try:
        return json.dumps(data, ensure_ascii=False)
    except (TypeError, ValueError):
        return default


def safe_json_loads(data: Any, default: Any = None) -> Any:
    """`data` parsed; `default` (or `{}`) if it will not parse.

    An already-parsed dict or list passes through: the driver decodes `jsonb`
    for us, but not `text` columns holding JSON, and callers see both.
    """
    if data is None:
        return default if default is not None else {}
    if isinstance(data, (dict, list)):
        return data
    if isinstance(data, str):
        try:
            return json.loads(data)
        except (json.JSONDecodeError, TypeError):
            return default if default is not None else {}
    return default if default is not None else {}


def parse_date(date_str: str, default: datetime | None = None) -> datetime | None:
    """The first of `DATE_FORMATS` that matches, else `default`."""
    if not date_str or not isinstance(date_str, str):
        return default
    date_str = date_str.strip()
    if not date_str:
        return default
    for fmt in DATE_FORMATS:
        if fmt == "%Y%m%d" and not (len(date_str) == 8 and date_str.isdigit()):
            # strptime reads 2026041 as 2026-04-01: one digit short is no date.
            continue
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    return default


def get_utc_now() -> datetime:
    """Now in UTC, naive: the shape the timestamp columns store."""
    return datetime.now(ZoneInfo("UTC")).replace(tzinfo=None)
