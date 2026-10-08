# `translate/aggregate`: daily summaries from the device point buffer

A device sends points (`series_data`: one row per person, indicator, source
and instant). This package turns a day of points into that day's figures
(`dailyAvgHeartRates`, `dailyTotalSteps`, ...), writes them as day-grained
observations through `collect/observations.py`, and elects which source's
figure each day publishes.

## Which day a point belongs to

The catalogue decides, per metric: `metrics.METRICS[name].window` is `00:00`
for a plain calendar day and `18:00` for a night (the sleep stages,
`napDuration`), which is dated by the evening it opened. `windows.py` turns
that into one SQL branch per window; the branches partition the input, so a
point is counted once. Names were once split with `LOWER(indicator) LIKE
'%sleep%'`, which moved 58 vendor-dated `daily…Sleep…` figures a day and
missed `napDuration`.

A day's bounds:

* **begin**: computed in SQL (`windows.day_begin_expression`): the point's
  instant read in its row's `timezone`, stepped back by the window, dated,
  given the window's clock time and converted back to UTC;
* **end**: the same wall clock one local day later in that zone
  (`_local_days_later`), so a daylight-saving day is 23 or 25 hours.

For Asia/Shanghai, a point at local 2025-10-30 12:00 begins a plain day at
2025-10-30 00:00 local and a night at 2025-10-29 18:00 local.

## One pass

```
series_data
    ↓ get_trigger_tasks (updated since the cursor) / _get_tasks_for_user_date_range
CalculationTask: (person, indicator, rule, day begin, zone)
    ↓ calculate_batch_aggregations: per day begin, per person and zone
summary rows: daily{Method}{Indicator}.{source}, with unit and timezone
    ↓ AggregateDatabaseService → observations.ingest_legacy_rows
th_observation (an equal row is skipped, a changed one amends it)
    ↓ election.elect_range over the days written
th_day_authority
```

* **Incremental** (`AggregateIndicatorService.process_incremental`): every
  person's points of the last three months whose `update_time` is after the
  task's cursor; with no cursor, those updated in the last 24 hours. The task advances the cursor to the newest
  `update_time` it processed.
* **A date range** (`recalculate_date_range`): one person over any span, or
  everyone over at most 30 days; longer spans run in 30-day chunks.
  `ingest/services/repair_reconcile.py` uses it after a repair sweep.

## The methods

The rules come from `IndicatorInfo.aggregation_methods` of every SERIES
indicator (`rule_generator.py`); each method becomes `daily{Method}{Indicator}`
(`naming.build_indicator_name`, where `sum` is named `Total`, like `total`).

* In one GROUP BY statement per person-day: `avg`, `max`, `min`, `sum` /
  `total`, `count`, `stddev`, `variance`, `last`, `first`, `median`, `p95`,
  `time_of_max`, `time_of_min`, and the thresholds `pct_below_N`,
  `pct_above_N`, `tir_L_U`.
* With a query of their own: the hypoglycaemic events (`hypo_event_count`,
  `hypo_event_times`, `hypo_event_details`), `gmi_14d` (fourteen local days,
  at least 70 % sensor coverage), and `sleep_onset_latency`,
  `morning_hr_jump`, `nighttime_resting_hr`.

`value` is text. Every cast goes through `_num()`, which reads a value that
is not a number as NULL, so one stray point does not fail the statement. A
person-day that fails anyway is logged and skipped; the rest of the pass
goes on. Points stored under `task_id = "filtered_out_of_range"` (outside
their plausible range, `collect/ingest/services/upload_health.py`) are never
aggregated.

An aggregator hub (`apple_health`) records one event under several
`source_id`s, so for hub sources only one `source_id` per (person,
indicator, source) is read; other sources use `source_id` for time-sliced
pulls and are read whole.

## Election

`election.py` decides, once and on the write side, which observation a
(person, series, local day) publishes: a measurer before a profile echo,
then coverage, then measurement freshness, then the deployment's
`th_data_source_priority`. A candidate whose duration exceeds its window
(forty hours of sleep in one night) is rejected and the rejection is a row of
`th_check_result`. Every reader joins `th_day_authority`; none re-decides.

## Running it

| | |
| --- | --- |
| Schedule | every 4 minutes (`task.py`), registered by `startup.py` |
| Cursor and lock | the shared scheduler's (`mirobody/utils/scheduler.py`) |
| Range chunk | 30 days |

The tests are in the maintainers' local suite; the live-database ones need
`MIROBODY_TEST_PG_DSN`.
