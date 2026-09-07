-- The five columns that make a day of readings answerable without guessing.
--
-- `th_series_data.start_time` is a NAIVE LOCAL wall-clock timestamp, so
-- "which day is this reading on" was answered at READ time by casting it and
-- padding the window a day each way (see the `date_padded_naive` semantics the
-- query layer still reports when `local_date` is NULL). That padding is why a
-- window of 2025-06-01..2025-06-10 returned a 2025-05-31 bucket: correct-ish
-- for a chart, wrong for "what was my resting heart rate on the 1st".
--
--   local_date    the local day the reading belongs to, through the metric's
--                 own window (`metrics.METRICS[name].window` — "18:00" for the
--                 sleep family, so a night is one day and not two halves).
--                 Written once, at write time, by `pulse/readings.py`.
--   series_key    the physical stream: `indicator|source`. Two devices'
--                 step counts must never be summed together, and the key is
--                 what keeps them apart.
--   source_class  measurer | aggregator | manual | profile_echo
--                 (`mirobody.kernel.series.SOURCE_*`) — the first criterion of
--                 `series.elect`: a scale MEASURES a weight, a wearable's
--                 profile ECHOES the one the user typed in months ago.
--   fingerprint   a stable hash of the fields that carry meaning. A re-sync
--                 that changed nothing does not touch the row (`sink.changed`).
--   elected       this row is the day's published authority for its
--                 (indicator, local_date). Election happens ONCE, on the
--                 write side (`pulse/aggregate`); every reader just filters.
--
-- Re-runnable, as every file here must be.

ALTER TABLE th_series_data ADD COLUMN IF NOT EXISTS local_date   date;
ALTER TABLE th_series_data ADD COLUMN IF NOT EXISTS series_key   text;
ALTER TABLE th_series_data ADD COLUMN IF NOT EXISTS source_class text;
ALTER TABLE th_series_data ADD COLUMN IF NOT EXISTS fingerprint  text;
ALTER TABLE th_series_data ADD COLUMN IF NOT EXISTS elected      boolean NOT NULL DEFAULT false;

COMMENT ON COLUMN th_series_data.local_date   IS 'Local day through the metric window (18:00 for sleep); NULL means legacy row, read with date_padded_naive semantics';
COMMENT ON COLUMN th_series_data.series_key   IS 'indicator|source — the physical stream';
COMMENT ON COLUMN th_series_data.source_class IS 'measurer | aggregator | manual | profile_echo (series.SOURCE_*)';
COMMENT ON COLUMN th_series_data.fingerprint  IS 'stable hash of the meaningful fields; equal means nothing changed';
COMMENT ON COLUMN th_series_data.elected      IS 'the published daily authority for (indicator, local_date)';

-- The day-grained read path: one indicator, one day, elected rows only.
CREATE INDEX IF NOT EXISTS idx_th_series_data_local_date
    ON th_series_data (user_id, indicator, local_date)
    WHERE deleted = 0;

-- The election read: "what is today's authority for this indicator".
CREATE INDEX IF NOT EXISTS idx_th_series_data_elected
    ON th_series_data (user_id, local_date, indicator)
    WHERE deleted = 0 AND elected;
