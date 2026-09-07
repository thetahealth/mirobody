#!/usr/bin/env python3
"""End-to-end check of the health-data path against a REAL database.

The unit tests pin the tool's contract without a store (`tests/agent/tools/
test_health_indicators_service.py`) and the SQL's shape without a connection
(`tests/pulse/test_query.py`). This script is the third thing: it runs the whole
path — subject zone, window, election, rendering, the three medication views and the
PHI sentinel — against a live Postgres with real rows in it, because the two
failures that only a database shows are a statement that will not parse and a
column that is not there.

    docker compose exec mirobody python -m scripts.e2e_health_data --user 1
    # or, from a checkout with a reachable database:
    python scripts/e2e_health_data.py --user 1

Exit status is the number of failed checks, so it is usable in CI.

`--capture DIR` additionally writes each answer as JSON, which is how the
pre-rewrite behaviour of the tool this replaced was recorded: run it against
the old code, keep the directory, diff it after the change.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

#: The demo values the PHI sentinel looks for — defined once, in the seed that
#: writes them (`mirobody.server.demo`). A reading no real person would have and a
#: comment nobody would write: if either reaches a log line, the redaction is
#: not working, and grepping for "a number" would never show it.
def phi_canary() -> str:
    from mirobody.server.demo import PHI_CANARY
    return PHI_CANARY

CASES: list[tuple[str, dict]] = [
    ("catalog", {}),
    ("catalog_windowed", {"start": "2025-06-01", "end": "2025-06-30"}),
    ("readings", {"indicators": ["RestingHeartRate-RHR"], "limit": 5}),
    ("readings_stringified", {"indicators": '["RestingHeartRate-RHR"]', "limit": 3}),
    ("buckets_day", {"indicators": ["RestingHeartRate-RHR"], "resolution": "day",
                     "start": "2025-06-01", "end": "2025-06-10"}),
    ("buckets_month", {"indicators": ["RestingHeartRate-RHR"], "resolution": "month",
                       "start": "2025-01-01", "end": "2025-12-31"}),
    ("buckets_hour", {"indicators": ["RestingHeartRate-RHR"], "resolution": "hour",
                      "start": "2025-06-01", "end": "2025-06-03"}),
    ("stats_readings", {"indicators": ["RestingHeartRate-RHR"], "aggregate": "stats"}),
    ("stats_daily", {"indicators": ["RestingHeartRate-RHR"], "resolution": "day", "aggregate": "stats"}),
    ("stats_text_value", {"indicators": ["UrineGlucose-GLU"], "aggregate": "stats"}),
    ("latest", {"indicators": ["RestingHeartRate-RHR"], "aggregate": "latest"}),
    ("keywords", {"keywords": ["resting heart"], "limit": 3}),
    ("keywords_miss", {"keywords": ["definitely-not-an-indicator-zzz"], "limit": 3}),
    ("refused_catalog_resolution", {"resolution": "day"}),
    ("refused_wrong_kind", {"kind": "medications", "resolution": "day"}),  # the former mode switch
    ("refused_bad_limit", {"indicators": ["x"], "limit": 99999}),
]


async def _pick_a_day(service, user_id: str) -> tuple[str, str] | None:
    """An (indicator, local date) this subject has a numeric reading for."""
    catalogue = await service.envelope({"user_id": user_id})
    for row in catalogue.data or []:
        try:
            float(str(row.get("latest_value") or ""))
        except ValueError:
            continue
        if row.get("last_date"):
            return str(row["indicator"]), str(row["last_date"])
    return None


async def _canary_present(user_id: str, canary: str) -> bool:
    from mirobody.utils import execute_query

    rows = await execute_query(
        "SELECT 1 FROM th_series_data WHERE user_id = :uid AND deleted = 0"
        " AND decrypt_content(comment) LIKE :needle LIMIT 1",
        {"uid": str(user_id), "needle": f"%{canary}%"},
        log_sql=False,
    ) or []
    return bool(rows)


def _check(name: str, ok: bool, detail: str = "") -> bool:
    print(f"  {'ok  ' if ok else 'FAIL'}  {name}{(' — ' + detail) if detail and not ok else ''}")
    return ok


async def run(user_id: str, capture: Path | None) -> int:
    from mirobody.utils import Config

    await Config.init(yaml_filenames=[])

    from mirobody.kernel import query, tools
    from mirobody.agent.tools.health_indicators_service import HealthIndicatorsService, render_compact, render_rest
    from mirobody.agent.tools.medications_service import MedicationsService

    service = HealthIndicatorsService()
    failures = 0
    print(f"\nquery_health_indicators against user {user_id}\n")

    for name, args in CASES:
        envelope = await service.envelope({"user_id": user_id}, **args)
        text = render_compact(envelope)
        if capture:
            capture.mkdir(parents=True, exist_ok=True)
            (capture / f"{name}.json").write_text(
                json.dumps({"rendered": text, "rest": render_rest(envelope)},
                           ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n",
                encoding="utf-8",
            )
        expect_error = name.startswith("refused_")
        got_error = envelope.status == tools.STATUS_ERROR
        if not _check(name, got_error == expect_error, f"status={envelope.status} kind={envelope.error_kind}"):
            failures += 1
            continue
        if expect_error:
            failures += not _check(f"{name}: recoverable", envelope.error_class == tools.ERROR_RECOVERABLE)
            failures += not _check(f"{name}: names the parameter", "invalid_arguments" == envelope.error_kind)
        else:
            failures += not _check(f"{name}: renders", bool(text.strip()))
            failures += not _check(
                f"{name}: window semantics declared",
                envelope.meta.window_semantics in (query.SEMANTICS_TZ_EXACT, query.SEMANTICS_DATE_PADDED),
            )

    print("\ninvariants\n")
    # A day-grained answer must agree with the day authority the dashboard
    # reads. Which indicator and which day come from what this subject ACTUALLY
    # has: a hard-coded pair passes vacuously on a record that does not carry it.
    probe = await _pick_a_day(service, user_id)
    if probe is None:
        failures += not _check("one day, one number (bucket == latest)", False, "no numeric reading to compare")
    else:
        indicator, day_iso = probe
        day = await service.envelope(
            {"user_id": user_id}, indicators=[indicator], resolution="day", start=day_iso, end=day_iso
        )
        latest = await service.envelope(
            {"user_id": user_id}, indicators=[indicator], aggregate="latest", start=day_iso, end=day_iso
        )
        day_value = (list(day.data or [{}])[0] or {}).get("avg")
        latest_value = (list(latest.data or [{}])[0] or {}).get("value")
        failures += not _check(
            f"one day, one number ({indicator} on {day_iso})",
            day_value is not None and latest_value is not None
            and abs(float(day_value) - float(latest_value)) < 1e-6,
            f"bucket={day_value} latest={latest_value}",
        )

    # The baseline is the OLDEST reading, and it comes from `aggregate=stats`.
    #
    # This replaces a `("readings_oldest", {..., "order": "oldest"})` case that
    # 1.4.0 made unpassable — `order` is not in `query.TOOL_SCHEMA` any more, so
    # it was refused as an unknown parameter on every run, and the loop above
    # counted that as a failure forever. A check that can never pass is worse
    # than no check: it turns red into background noise.
    #
    # Renaming it to `aggregate=stats` would have duplicated `stats_readings`
    # exactly — the loop only asserts "it rendered". What the old case actually
    # verified is that you can reach the FIRST reading, and that is what is
    # asserted here instead: `first`/`first_date` are present, and the baseline
    # is not newer than the latest value it is a baseline for.
    # The indicator comes from `probe`, not from a literal. A hard-coded name
    # this subject does not carry answers with the CATALOGUE instead of with
    # readings, and every assertion below then grades the wrong path — the same
    # vacuity `_pick_a_day` exists to avoid, which is how the first draft of
    # this check "failed" against a record that simply has other indicators.
    baseline = await service.envelope(
        {"user_id": user_id}, indicators=[probe[0]], aggregate="stats"
    ) if probe else None
    row = (list(baseline.data or [{}])[0] or {}) if baseline else {}
    has_baseline = row.get("first") is not None and row.get("first_date") is not None
    failures += not _check(
        "a baseline is the oldest reading (aggregate=stats -> first/first_date)",
        has_baseline,
        f"first={row.get('first')!r} first_date={row.get('first_date')!r}"
        if baseline else "no numeric reading to build a baseline from",
    )
    if has_baseline and row.get("last_date") is not None:
        failures += not _check(
            "the baseline is not newer than the latest",
            str(row["first_date"]) <= str(row["last_date"]),
            f"first_date={row['first_date']} last_date={row['last_date']}",
        )

    # Medications are their own tool; the three views must all answer.
    medications = MedicationsService()
    for view in ("plan", "log", "history"):
        env = await medications.envelope({"user_id": user_id}, view=view)
        failures += not _check(f"medications_{view}", env.status != tools.STATUS_ERROR, f"status={env.status} kind={env.error_kind}")

    # An unknown parameter is refused BY NAME, or the model cannot fix its call.
    refusal = await service.envelope({"user_id": user_id}, kind="medications", resolution="day")
    failures += not _check("an unknown parameter is refused by name", "kind: unknown parameter" in "; ".join(refusal.assumptions))

    # The tool must never raise: the REPL calls it with no middleware above it.
    class Broken:
        def tz(self, _):
            raise RuntimeError("the store fell over")

    broken = HealthIndicatorsService(Broken())
    env = await broken.envelope({"user_id": user_id})
    failures += not _check("never raises", env.status == tools.STATUS_ERROR)
    failures += not _check("never leaks the exception message", "fell over" not in render_compact(env))

    # The PHI sentinel. The tool must be able to READ the canary and must not
    # log it: `phi_lint` checks the source and `ops.PHIPolicy` filters at
    # runtime, and this is the only check that exercises the whole system.
    # The PHI sentinel's precondition: the canary must BE in the record, or a
    # log that does not contain it proves nothing. Checked against the column
    # directly — the tool deliberately never returns a comment, which is
    # exactly why a comment is what the canary rides on.
    canary = phi_canary()
    failures += not _check(
        "the canary is in the record (so a log check means something)",
        await _canary_present(user_id, canary),
        "seed the demo data first (SEED_DEMO_DATA=true), or pass the member's --user",
    )
    print(f"\n  now grep the running container's logs — a hit is a leak:\n"
          f"    docker compose logs mirobody | grep -F {canary.split()[-1]!r} | head\n")

    print(f"\n{failures} failed\n")
    return failures


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--user", default="1", help="subject id to read (default: the demo user)")
    parser.add_argument("--capture", type=Path, default=None, help="write each answer as JSON into this directory")
    args = parser.parse_args()
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    return asyncio.run(run(args.user, args.capture))


if __name__ == "__main__":
    raise SystemExit(main())
