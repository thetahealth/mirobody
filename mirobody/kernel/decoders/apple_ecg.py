"""An Apple Watch ECG recording: `electrocardiograms/ecg_YYYY-MM-DD.csv`.

The Health app writes one file per recording under `electrocardiograms/` in
an export, outside `export.xml` (`apple_export.open_export` reads only the
top-level export folder; the DTD comment it quotes is about `Correlation`,
not this). A small `key,value` header (name, date of birth, the recorded
instant with its own offset, the rhythm classification, symptoms, software
version, device, sample rate, then the lead and its unit) is followed by
about 15,360 microvolt samples (30 s at 512 Hz).

Pure: stdlib only. `parse` reads the header and stops; the samples are never
decoded into anything here, because the only use a model or a store ever had
for them was wrong (issue #334's reported case sent them whole to a model and
threw the answer away, keeping one uncoded word). `name` and `date_of_birth`
have no field on `EcgReading` for the same reason: issue #334's Expected
section is explicit that neither becomes a stored reading.
"""

from __future__ import annotations

import csv
import io
from dataclasses import dataclass
from datetime import datetime

#: Apple writes `"2026-11-15 08:12:30 +0800"`: a space, then a numeric
#: offset, never a bare "Z" or an IANA name. Read literally, as
#: `apple.py`'s own `_TS` reads `export.xml`'s timestamps for the same reason
#: (the watch's own clock offset, not a fabricated one).
_TS = "%Y-%m-%d %H:%M:%S %z"

#: The eight metadata rows, in this order, before the lead and its unit.
#: Undocumented by Apple; read off a reported export (issue #334).
HEADER_FIELDS: tuple[str, ...] = (
    "Name", "Date of Birth", "Recorded Date", "Classification",
    "Symptoms", "Software Version", "Device", "Sample Rate",
)
LEAD_FIELD = "Lead"
UNIT_FIELD = "Unit"

#: Physical rows read before giving up: the header plus the lead/unit pair
#: and a few blank separators, never far enough to reach a sample. A 30 s
#: recording holds 15,360 of them; this module must never hold even one.
_MAX_ROWS = 32


@dataclass(frozen=True)
class EcgReading:
    """One recording's header, nothing else. `recorded_at` is aware, in the
    offset the watch itself wrote; `sample_rate_hz` is `None` when the
    printed rate does not parse, which never blocks a reading on its
    classification and time."""

    classification: str
    recorded_at: datetime
    device: str
    software_version: str
    symptoms: str
    sample_rate_hz: float | None
    lead: str
    unit: str


def _rows(text: str) -> list[list[str]]:
    """Up to `_MAX_ROWS` CSV rows of `text`, stopping well short of the
    waveform. `csv.reader` (not a comma split) so a quoted field carrying its
    own comma (`Device,"Watch7,1"`, a birth date `"Jan 1, 1980"`) is read as
    one cell, not two."""
    out: list[list[str]] = []
    for row in csv.reader(io.StringIO(text)):
        out.append(row)
        if len(out) >= _MAX_ROWS:
            break
    return out


def _as_dict(rows: list[list[str]]) -> dict[str, str]:
    return {r[0]: (r[1] if len(r) > 1 else "") for r in rows if r}


def _has_header(rows: list[list[str]]) -> bool:
    keys = [r[0] for r in rows if r]
    return keys[: len(HEADER_FIELDS)] == list(HEADER_FIELDS)


def sniff(text: str) -> bool:
    """Whether `text` opens like an Apple Watch ECG export: its first eight
    non-blank rows name exactly `HEADER_FIELDS`, in order. Never true of an
    ordinary lab CSV, which does not open with a literal `Name` row."""
    return _has_header(_rows(text))


def _sample_rate(value: str) -> float | None:
    # "512 hertz" -> 512.0. The word after the number is never a UCUM unit
    # this project converts: the samples it names are not stored, so there
    # is nothing here to convert into (`kernel/metrics.py`'s rule that a
    # decoder converts INTO the catalogue's unit does not apply to a
    # quantity the catalogue never carries).
    head = value.strip().split(" ", 1)[0]
    try:
        return float(head)
    except ValueError:
        return None


def _recorded_at(value: str) -> datetime | None:
    try:
        return datetime.strptime(value.strip(), _TS)
    except ValueError:
        return None


def parse(text: str) -> EcgReading | None:
    """The one reading in an Apple Watch ECG export, or `None` when `text`
    is not this shape, or the two fields a reading needs (a classification
    and a recorded instant) are missing or unparsable. Nothing is ever
    guessed in: an unreadable `Recorded Date` is a `None`, never `now()`."""
    rows = _rows(text)
    if not _has_header(rows):
        return None
    header = _as_dict(rows[: len(HEADER_FIELDS)])
    classification = header.get("Classification", "").strip()
    recorded_at = _recorded_at(header.get("Recorded Date", ""))
    if not classification or recorded_at is None:
        return None
    tail = _as_dict([r for r in rows[len(HEADER_FIELDS):] if r and r[0] in (LEAD_FIELD, UNIT_FIELD)])
    return EcgReading(
        classification=classification,
        recorded_at=recorded_at,
        device=header.get("Device", "").strip(),
        software_version=header.get("Software Version", "").strip(),
        symptoms=header.get("Symptoms", "").strip(),
        sample_rate_hz=_sample_rate(header.get("Sample Rate", "")),
        lead=tail.get(LEAD_FIELD, "").strip(),
        unit=tail.get(UNIT_FIELD, "").strip(),
    )


#: Rows left out of `header_text`: issue #334's Expected section says these
#: two are never stored, not only never turned into a reading.
_UNSTORED_FIELDS = frozenset({"Name", "Date of Birth"})


def header_text(text: str) -> str:
    """`text`'s metadata rows up to and including `Unit`, with their own line
    breaks, `Name` and `Date of Birth` left out. What a reader should see
    stored for this file: the ~15,360 samples after the header are not
    prose, and this module's job is to never hold them anyway."""
    lines = text.splitlines(keepends=True)
    kept: list[str] = []
    for line in lines[:_MAX_ROWS]:
        row = next(csv.reader([line]), [])
        if row and row[0] in _UNSTORED_FIELDS:
            continue
        kept.append(line)
        if row and row[0] == UNIT_FIELD:
            break
    return "".join(kept)


__all__ = ["HEADER_FIELDS", "LEAD_FIELD", "UNIT_FIELD", "EcgReading", "header_text", "parse", "sniff"]
