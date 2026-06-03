-- Mirror of theta_ai.standard_indicators, populated by
-- RegisterStandardIndicatorsTask from the in-code StandardIndicator enum
-- and rule_generator's derived aggregation rules.
--
-- Kept separate from standard_indicators so the hand-curated lab/biomarker
-- catalog (78 rows with name_zh, reference ranges, unit_alternatives) is not
-- overwritten by auto-registered device/wearable indicators. Schema mirrors
-- standard_indicators verbatim so a future UNION/merge is trivial.

CREATE TABLE IF NOT EXISTS theta_ai.standard_indicators_device (
    id                text PRIMARY KEY
                       CHECK (char_length(id) >= 2 AND char_length(id) <= 60),
    name_en           text NOT NULL,
    name_zh           text NOT NULL,
    kind              text NOT NULL DEFAULT 'scalar',
    system            text NOT NULL,
    canonical_unit    text,
    specimen_type     text,
    reference_low     numeric,
    reference_high    numeric,
    unit_alternatives jsonb DEFAULT '[]'::jsonb,
    is_active         boolean DEFAULT true,
    created_at        timestamp with time zone DEFAULT now(),
    updated_at        timestamp with time zone DEFAULT now()
);
