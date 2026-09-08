"""The keys `GET /api/v1/health-indicators` puts on the wire.

`docs/frontend.md`: "The API is the contract, the client is one consumer of
it — if you want a different UI, build it against the same surface." This
module is that sentence's evidence. A consumer cannot read a key that is not
here, and until this existed nothing in THIS repository noticed when one moved:
`c470b3d` reshaped the response around one envelope, the shipped bundle read
`catalog` / `indicators`, and the Indicators tab rendered "No indicators yet"
over an account with data while the suite stayed green (#62).

Every assertion below is on an EXACT key set, not on presence. A renamed or
dropped key is the whole failure mode, and `assert "rows" in payload` passes
for a payload that also stopped sending `unit`.

It pins the serialization only — `render_rest` and the two row builders it
carries — with no database. What the rows CONTAIN is `pulse`'s contract and is
tested there; what they are CALLED is this boundary's, and is what breaks a
client.
"""

from __future__ import annotations

from mirobody.agent.tools.health_indicators_service import render_rest
from mirobody.kernel import tools
from mirobody.pulse.query import _catalog_row, _reading_row
from mirobody.server.routers.indicator_router import ReadingPatch

# The rows a client maps, one per grain. Field for field what the client repo's
# rows.test.js fixtures carry, so the two suites cannot drift apart silently.
CATALOG_KEYS = {
    "indicator",
    "system",
    "code",
    "count",
    "unit",
    "latest_value",
    "first_date",
    "last_date",
    "total",
    "day_known",
}

READING_KEYS = {
    "indicator",
    "time",
    "value",
    "unit",
    "file_key",
    "row_id",
    "system",
    "code",
    "total",
    "day_known",
    "provenance",
}

ENVELOPE_KEYS = {
    "rows",
    "count",
    "total",
    "truncated",
    "window",
    "resolution",
    "aggregate",
    "status",
}


def _envelope(rows, **meta):
    return tools.Envelope(tools.STATUS_OK, data=rows, meta=tools.Meta(**meta))


def test_the_envelope_carries_exactly_these_keys():
    payload = render_rest(_envelope([]))
    assert set(payload) == ENVELOPE_KEYS


def test_error_kind_appears_only_on_an_error():
    ok = render_rest(_envelope([]))
    assert "error_kind" not in ok

    failed = render_rest(tools.invalid_arguments("no such indicator"))
    assert set(failed) == ENVELOPE_KEYS | {"error_kind"}


def test_rows_is_where_the_data_is():
    """The key #62 was about. `catalog` and `indicators` are not alternatives
    this route ever accepted — a consumer reading either gets nothing, and gets
    it with a 200."""
    payload = render_rest(_envelope([_catalog_row({"indicator": "hemoglobin", "count": 1})]))
    assert payload["rows"] == [_catalog_row({"indicator": "hemoglobin", "count": 1})]
    assert "catalog" not in payload
    assert "indicators" not in payload


def test_a_catalog_row_carries_exactly_these_keys():
    assert set(_catalog_row({"indicator": "bodyMasss"})) == CATALOG_KEYS


def test_a_reading_row_carries_exactly_these_keys():
    assert set(_reading_row({"indicator": "bodyMasss"}, {})) == READING_KEYS


def test_the_two_grains_stay_distinguishable_from_the_payload_alone():
    """One path answers in two grains — catalog without `keywords`/`indicators`,
    readings with them — and a search matching nothing answers with the CATALOG,
    so a consumer cannot infer the grain from its own request. It sniffs the
    rows instead: only a reading has `row_id`, only a catalog row has
    `last_date`. Both halves of that test must keep working."""
    catalog = _catalog_row({"indicator": "bodyMasss", "last_date": "2025-04-07"})
    reading = _reading_row({"indicator": "bodyMasss", "id": 558281}, {})

    assert "row_id" not in catalog
    assert reading["row_id"] == 558281
    assert catalog["last_date"] == "2025-04-07"
    assert "last_date" not in reading


def test_a_reading_names_its_row_id_and_the_edit_route_takes_id():
    """The asymmetry that hid the edit and delete buttons from every row.

    A reading arrives as `row_id`; `POST /health-indicators/reading` takes the
    same th_series_data id as `id`. A client that reads `row.id` off the payload
    gets `undefined`, and the UI offers those controls only when the id is
    present — so owner-only correction silently disappeared, and no error was
    logged anywhere because nothing failed. Renaming either side without the
    other reintroduces it."""
    reading = _reading_row({"indicator": "bodyMasss", "id": 558281}, {})
    assert reading["row_id"] == 558281
    assert "id" not in reading

    assert ReadingPatch(id=reading["row_id"], value="69.6").id == 558281
    assert "row_id" not in ReadingPatch.model_fields


def test_truncated_does_not_mean_rows_were_dropped():
    """`truncated` rides on `_per_indicator_truncated`, which compares each
    row's `total` — the count over that indicator's WHOLE series — against the
    rows carrying it. A complete catalog therefore answers `truncated: true`,
    and a consumer that surfaces the flag as "some data is missing" says so on
    a full page. `total` against the row count is the honest test."""
    payload = render_rest(_envelope([{"indicator": "bodyMasss"}], row_count=1, truncated=True, catalog_total=1))
    assert payload["truncated"] is True
    assert payload["total"] == payload["count"] == 1


def test_the_meta_block_says_which_days_it_answered_for():
    """`window` is an object, not four sibling keys: a client renders "readings
    for 2025-01-01..2025-04-07" from it, and `semantics` is the difference
    between "on the 3rd" and "around the 3rd" for rows with no stored local
    day."""
    payload = render_rest(_envelope([], window=("2025-01-01", "2025-04-07"), tz="Asia/Shanghai"))
    assert payload["window"] == {
        "start": "2025-01-01",
        "end": "2025-04-07",
        "tz": "Asia/Shanghai",
        "semantics": "tz_exact",
    }
