"""A permission scope must not stand in for an unverified measurement field."""

from mirobody.server.routers.records_router import _device_metric
from mirobody.standardize import devices


def test_honor_distance_scope_does_not_claim_an_elevation_field():
    # Honor's daily statistics list DISTANCE and ALTITUDE as two data types
    # under one scope, distance.read (scopeId 408): the rows are the types,
    # and the scope names none of them.
    honor = devices.vendor_fields("honor")
    assert not [row for row in honor if row.type == "distance.read"]
    rows = {row.type: row for row in honor if row.type in ("DISTANCE (daily)", "ALTITUDE (daily)")}
    assert set(rows) == {"DISTANCE (daily)", "ALTITUDE (daily)"}
    elevation = rows["ALTITUDE (daily)"]
    assert not elevation.metric and elevation.loinc == "101687-2"
    assert "【未映射：厂商字段未核实】" in elevation.note
    assert _device_metric("honor", "distance.read") == ("distance.read", None)
    assert _device_metric("honor", "ALTITUDE (daily)") == ("ALTITUDE (daily)", None)
