ALTER TABLE theta_ai.th_series_data ADD COLUMN IF NOT EXISTS full_dim_id bigint;
ALTER TABLE theta_ai.th_series_data ADD COLUMN IF NOT EXISTS fhir_id bigint;

COMMENT ON COLUMN theta_ai.th_series_data.full_dim_id IS '关联 indicator_full_dim 表的ID';
COMMENT ON COLUMN theta_ai.th_series_data.fhir_id IS 'FHIR标准指标ID，来自 indicator_full_dim.fhir_id';

CREATE INDEX IF NOT EXISTS idx_th_series_data_full_dim_id ON theta_ai.th_series_data (full_dim_id);
CREATE INDEX IF NOT EXISTS idx_th_series_data_fhir_id ON theta_ai.th_series_data (fhir_id);
