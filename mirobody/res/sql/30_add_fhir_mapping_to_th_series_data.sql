-- Add columns for FHIR mapping and data standardization
ALTER TABLE theta_ai.th_series_data ADD COLUMN IF NOT EXISTS fhir_mapping_info JSONB;
ALTER TABLE theta_ai.th_series_data ADD COLUMN IF NOT EXISTS is_standardized BOOLEAN DEFAULT FALSE;
ALTER TABLE theta_ai.th_series_data ADD COLUMN IF NOT EXISTS value_standardized TEXT;
