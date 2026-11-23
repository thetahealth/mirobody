"""`kernel.decoders.apple_ecg`: an Apple Watch ECG CSV's header, no model.

The header shape is issue #334's reported case: Name, Date of Birth,
Recorded Date (with its own offset), Classification, Symptoms, Software
Version, Device, Sample Rate, a blank separator, Lead, Unit, then the
microvolt samples. Every value here is synthetic.
"""

from __future__ import annotations

from datetime import timedelta, timezone

from mirobody.kernel.decoders import apple_ecg

_SAMPLES = "\n".join(f"{0.001 * i:.3f}" for i in range(300))


def _csv(*, blank_sep: str = ",", classification: str = "Sinus Rhythm",
         recorded_date: str = "2026-11-15 08:12:30 +0800") -> str:
    return (
        "Name,Test Person\n"
        'Date of Birth,"Jan 1, 1980"\n'
        f"Recorded Date,{recorded_date}\n"
        f"Classification,{classification}\n"
        "Symptoms,\n"
        "Software Version,2\n"
        'Device,"Watch7,1"\n'
        "Sample Rate,512 hertz\n"
        f"{blank_sep}\n"
        "Lead,Lead I\n"
        "Unit,µV\n"
        f"{blank_sep}\n"
        f"{blank_sep}\n"
        f"{_SAMPLES}\n"
    )


def test_sniff_recognizes_the_apple_ecg_header():
    assert apple_ecg.sniff(_csv()) is True


def test_sniff_recognizes_the_header_with_truly_blank_separators():
    # Apple's own export likely uses empty lines rather than a lone comma
    # between the metadata rows and `Lead`/`Unit` (reverse-engineered from a
    # third-party CSV reader's fixed row-skip count); both shapes must sniff.
    assert apple_ecg.sniff(_csv(blank_sep="")) is True


def test_sniff_rejects_an_ordinary_report():
    assert apple_ecg.sniff("Date,Weight\n2026-01-01,70\n2026-01-02,71\n") is False
    assert apple_ecg.sniff("Name,Classification\nAlice,Sinus Rhythm\n") is False
    assert apple_ecg.sniff("") is False


def test_parse_reads_the_classification_and_the_recorded_offset():
    reading = apple_ecg.parse(_csv())
    assert reading is not None
    assert reading.classification == "Sinus Rhythm"
    assert reading.recorded_at.utcoffset() == timedelta(hours=8)
    assert reading.recorded_at.replace(tzinfo=None).isoformat() == "2026-11-15T08:12:30"
    assert reading.device == "Watch7,1"
    assert reading.software_version == "2"
    assert reading.sample_rate_hz == 512.0
    assert reading.lead == "Lead I"
    assert reading.unit == "µV"
    assert reading.symptoms == ""


def test_parse_reads_a_negative_offset_and_a_different_classification():
    reading = apple_ecg.parse(_csv(classification="Inconclusive", recorded_date="2026-11-16 21:40:05 -0500"))
    assert reading is not None
    assert reading.classification == "Inconclusive"
    assert reading.recorded_at.utcoffset() == timedelta(hours=-5)


def test_parse_returns_none_without_a_classification():
    assert apple_ecg.parse(_csv(classification="")) is None


def test_parse_returns_none_with_an_unparsable_recorded_date():
    assert apple_ecg.parse(_csv(recorded_date="not a date")) is None


def test_parse_returns_none_for_an_ordinary_report():
    assert apple_ecg.parse("Date,Weight\n2026-01-01,70\n2026-01-02,71\n") is None
    assert apple_ecg.parse("") is None


def test_parse_never_turns_a_sample_into_a_reading():
    """The one thing this module must never do: the sample lines never reach
    `EcgReading` at all, let alone its classification."""
    reading = apple_ecg.parse(_csv())
    assert reading is not None
    for field in ("classification", "device", "software_version", "symptoms", "lead", "unit"):
        value = getattr(reading, field)
        assert "0.0" not in value and "0.1" not in value


def test_reading_has_no_field_for_name_or_date_of_birth():
    # Issue #334's Expected section: neither is stored, not only never a
    # reading. There is no field to accidentally carry either through.
    import dataclasses

    fields = {f.name for f in dataclasses.fields(apple_ecg.EcgReading)}
    assert "name" not in fields
    assert "date_of_birth" not in fields


def test_header_text_excludes_name_and_date_of_birth_and_every_sample():
    text = _csv()
    header = apple_ecg.header_text(text)
    assert "Test Person" not in header
    assert "Jan 1, 1980" not in header
    assert "Classification,Sinus Rhythm" in header
    assert "Recorded Date,2026-11-15 08:12:30 +0800" in header
    assert header.rstrip().endswith("Unit,µV")
    assert _SAMPLES not in header


def test_header_text_stops_before_the_samples_with_blank_separators():
    header = apple_ecg.header_text(_csv(blank_sep=""))
    assert header.rstrip().endswith("Unit,µV")
    assert _SAMPLES not in header


def test_offset_formats_a_fixed_zone_zero_padded():
    reading = apple_ecg.parse(_csv(recorded_date="2026-11-15 08:12:30 +0530"))
    assert reading is not None
    assert reading.recorded_at.utcoffset() == timedelta(hours=5, minutes=30)
    assert reading.recorded_at.tzinfo == timezone(timedelta(hours=5, minutes=30))
