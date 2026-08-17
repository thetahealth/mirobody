ALTER TABLE th_series_data ADD COLUMN IF NOT EXISTS full_dim_id bigint;
ALTER TABLE th_series_data ADD COLUMN IF NOT EXISTS fhir_id bigint;

COMMENT ON COLUMN th_series_data.full_dim_id IS 'indicator_full_dim.id';
COMMENT ON COLUMN th_series_data.fhir_id IS 'indicator_full_dim.fhir_id';

CREATE INDEX IF NOT EXISTS idx_th_series_data_full_dim_id ON th_series_data (full_dim_id);
CREATE INDEX IF NOT EXISTS idx_th_series_data_fhir_id ON th_series_data (fhir_id);
