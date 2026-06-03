-- TH-418 follow-up: add bilingual description columns to
-- theta_ai.standard_indicators_device so the SQL-queryable catalog
-- carries the human-readable descriptions that already exist in the
-- in-code IndicatorInfo (`description` / `description_zh`).
--
-- Nullable matches the standard_indicators specimen_type convention —
-- some IndicatorInfo entries have description = "" (older entries) and
-- we prefer NULL in DB over empty strings.

ALTER TABLE theta_ai.standard_indicators_device
    ADD COLUMN IF NOT EXISTS description    text,
    ADD COLUMN IF NOT EXISTS description_zh text;
