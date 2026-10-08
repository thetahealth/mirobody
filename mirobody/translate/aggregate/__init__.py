"""Daily summaries from the device point buffer, and which source a day publishes.

    rule_generator.py   the rules, read off IndicatorInfo.aggregation_methods
    windows.py          which local day a point belongs to (the catalogue's window)
    aggregators/        the SQL that computes a day's figures from series_data
    database_service.py writes them as observations
    election.py         which source's figure the day publishes
    service.py          one incremental pass, or a date range
    task.py, startup.py the scheduled job

Nothing is re-exported here: importing a submodule must not load the rest.
The entry points other packages use are `mirobody.translate`'s
(`AggregateIndicatorService`, `start_aggregate_indicator_scheduler`, ...).
See README.md.
"""
