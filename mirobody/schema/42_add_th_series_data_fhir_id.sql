-- th_series_data.fhir_id — the reading's terminology identity, and the only
-- part of ② Standardize that reaches a user: `JOIN fhir_indicators fi ON
-- tsd.fhir_id = fi.id` is what lets `_coding_for`
-- (agent/tools/health_indicator_service.py) hand the model a {system, code} per
-- indicator. Filled on ingest from the FhirMapping cache (upload_health.py,
-- sql_aggregator.py) and backfilled by task/indicator_sync.py. NULL means "not
-- resolved yet" — the pool 93_'s partial index exists to scan cheaply.
--
-- It holds `fhir_indicators.id`: a bigserial primary key, local to one
-- database. NOT the packed `system<<60 | code` that
-- `indicator/fhir/common.py:code_to_fhir_id` builds for the offline bundle.
-- Same name, different space — `fhir_id_to_code()` on this column returns
-- garbage, which is why the map that bridged them (`fhir_id_map.npy`) was
-- deleted from the repo rather than shipped.
--
-- This file also used to add `full_dim_id bigint` + an index on it, keying into
-- `indicator_full_dim` — a dimension table that no file here creates and that
-- belonged to a service this project no longer runs. Nothing in the repo ever
-- read or wrote the column, so the index maintained a b-tree over a column that
-- is NULL in every row, on every insert into the highest-write table in the
-- schema. It stops being created here; 99_ drops the index for databases that
-- already ran the old version. The column itself is left in place, per the
-- pruning policy in schema/README.md.
ALTER TABLE th_series_data ADD COLUMN IF NOT EXISTS fhir_id bigint;

COMMENT ON COLUMN th_series_data.fhir_id IS 'fhir_indicators.id';

CREATE INDEX IF NOT EXISTS idx_th_series_data_fhir_id ON th_series_data (fhir_id);
