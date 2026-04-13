-- Add embedding_model column to track which model was used for embedding generation
-- This helps with debugging and data migration when switching embedding providers

ALTER TABLE th_series_dim ADD COLUMN IF NOT EXISTS embedding_model text;
COMMENT ON COLUMN th_series_dim.embedding_model IS 'Embedding model used (e.g., dashscope/text-embedding-v4)';
