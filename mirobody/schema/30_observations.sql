-- 30_observations.sql: every reading, whatever brought it in, as one row.
--
--   th_extraction        the structured input a reading came from, frozen
--   th_observation       one atomic observation: the text as printed, the
--                        typed layer, the time with its zone. Append-only:
--                        a correction is a new row pointing at the old one
--   th_coding_current    the code and series it carries now (1:1)
--   th_coding_history    every coding it ever had, only appended
--   th_coding_decision   why a (name, unit, value kind) got its code, shared
--   th_coding_alias      mappings a person confirmed
--   th_concept           display names and axes of the codes in use
--   th_series            one row per (user, series): the catalogue an
--                        assistant reads first
--   th_day_authority     which observation a day publishes
--   th_check_result      consistency checks, as rows
--   v_observation        the one read surface
--   th_series_data_genetic   genotypes, a record of their own
--
-- Writer: collect/observations.py. Design: docs/pipeline.md section 6 and
-- internal/plans/1.5.x/2026-09-15-cta-data-architecture.md. Names are
-- th_ + singular noun + role; enum values are kebab-case. `note_text` is the
-- only encrypted column; the printed name, value and unit are stored clear.

CREATE TABLE IF NOT EXISTS th_extraction (
    id            bigserial PRIMARY KEY,
    user_id       varchar(200) NOT NULL,
    source_kind   text NOT NULL,                 -- file | device | manual | api
    source_ref    text NOT NULL,                 -- file_key | vendor batch id | request id
    extractor     text NOT NULL,                 -- 'llm:<provider>/<model>@draft-v1' | 'vendor:<name>' | 'manual'
    status        text NOT NULL,                 -- ok | failed | unverified
    reason        text NOT NULL DEFAULT '',
    payload       jsonb,                         -- the ObservationDraft, verbatim; NULL when failed
    digest        text NOT NULL,                 -- sha256(source_ref, extractor, payload)
    report_date   date,
    date_source   text NOT NULL DEFAULT '',      -- extracted | upload-time | manual
    created_at    timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT th_extraction_ref_digest UNIQUE (source_ref, digest),
    CONSTRAINT th_extraction_reason CHECK (status = 'ok' OR reason <> '')
);
CREATE INDEX IF NOT EXISTS idx_th_extraction_user ON th_extraction (user_id, created_at);

CREATE TABLE IF NOT EXISTS th_observation (
    id                 bigserial PRIMARY KEY,
    user_id            varchar(200) NOT NULL,
    kind               text NOT NULL,            -- measurement | symptom | finding | organizer
    modality           text NOT NULL,            -- lab-report | device-sensed | self-reported | manual | derived | llm-unverified
    source_kind        text NOT NULL,            -- file | device | manual | api
    source_ref         text NOT NULL,
    source_record_id   text,                     -- the vendor's own sample id, when it has one
    extraction_id      bigint REFERENCES th_extraction(id),
    row_ix             int,
    vendor             text,
    vendor_field       text,
    derived_from       bigint[],
    member_of          bigint REFERENCES th_observation(id),
    panel_text         text NOT NULL DEFAULT '',
    observed_start     timestamptz NOT NULL,
    observed_end       timestamptz NOT NULL,
    tz                 text NOT NULL,            -- IANA name, or UTC±HH:MM when only an offset was known
    tz_source          text NOT NULL,            -- iana | offset-only | floating | user-default
    local_date         date NOT NULL,            -- translate.local_day(), nowhere else
    grain              text NOT NULL,            -- instant | window | day
    stat               text NOT NULL DEFAULT '', -- Open mHealth descriptive statistic for window/day rows
    name_text          text NOT NULL,            -- as printed, never translated
    value_text         text NOT NULL DEFAULT '',
    unit_text          text NOT NULL DEFAULT '',
    ref_text           text NOT NULL DEFAULT '',
    flag_text          text NOT NULL DEFAULT '',
    method_text        text NOT NULL DEFAULT '',
    specimen_text      text NOT NULL DEFAULT '',
    note_text          text,                     -- ENCRYPTED free text; NULL when there is none
    name_key           text NOT NULL,            -- translate.fold.name_key(name_text)
    value_kind         text NOT NULL,            -- quantity | ordinal | nominal | narrative | absent
    value_num          numeric,
    comparator         text NOT NULL DEFAULT '', -- '' < <= > >=
    data_absent_reason text NOT NULL DEFAULT '',
    unit_ucum          text NOT NULL DEFAULT '', -- '' when there is no unit or it did not normalize
    ref_low            numeric,
    ref_high           numeric,
    local_key          text NOT NULL,            -- name_key|unit: the series of an uncoded reading
    stream_key         text NOT NULL,            -- local_key|source_kind:vendor: the physical stream
    source_class       text NOT NULL,            -- measurer | profile_echo | aggregator | manual
    fingerprint        text NOT NULL,
    status             text NOT NULL DEFAULT 'final',   -- final | entered-in-error
    amends             bigint REFERENCES th_observation(id),
    created_at         timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT th_observation_instant CHECK (grain <> 'instant' OR observed_start = observed_end),
    CONSTRAINT th_observation_stat CHECK (grain = 'instant' OR stat <> ''),
    CONSTRAINT th_observation_absent CHECK (value_kind <> 'absent' OR data_absent_reason <> ''),
    CONSTRAINT th_observation_quantity CHECK (value_kind <> 'quantity' OR value_num IS NOT NULL),
    CONSTRAINT th_observation_derived CHECK (modality <> 'derived' OR derived_from IS NOT NULL),
    CONSTRAINT th_observation_status CHECK (status = 'final' OR amends IS NOT NULL)
);
-- The identity of an observation: who, what (as printed), when, from which
-- document or batch, which vendor record, inside which panel. A correction
-- shares all of that and points at the row it amends, so `amends` is part of
-- the key. NULLs are distinct in a plain UNIQUE, hence the COALESCEs.
CREATE UNIQUE INDEX IF NOT EXISTS uq_th_observation_identity
    ON th_observation (user_id, name_key, observed_start, observed_end, source_ref,
                       COALESCE(source_record_id, ''), COALESCE(member_of, 0), COALESCE(amends, 0));
CREATE INDEX IF NOT EXISTS idx_th_observation_user_day ON th_observation (user_id, local_date);
CREATE INDEX IF NOT EXISTS idx_th_observation_stream ON th_observation (user_id, stream_key, observed_start);
CREATE INDEX IF NOT EXISTS idx_th_observation_source ON th_observation (source_ref);
CREATE INDEX IF NOT EXISTS idx_th_observation_amends ON th_observation (amends) WHERE amends IS NOT NULL;

CREATE TABLE IF NOT EXISTS th_coding_decision (
    decision_id  text PRIMARY KEY,               -- 16 hex over (name_key, unit_ucum, value_kind, release, rule)
    name_key     text NOT NULL,
    unit_ucum    text NOT NULL DEFAULT '',
    value_kind   text NOT NULL,
    release      text NOT NULL,
    rule         text NOT NULL,                  -- 'engine:lexical' | 'alias:user' | 'axes:unique' ...
    evidence     text[] NOT NULL DEFAULT '{}',
    rejected     jsonb,
    human        boolean NOT NULL DEFAULT false,
    created_at   timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS th_coding_current (
    observation_id   bigint PRIMARY KEY REFERENCES th_observation(id) ON DELETE CASCADE,
    release          text NOT NULL,
    outcome          text NOT NULL,              -- coded | needs-input | refused
    reason           text,
    code_system      text,
    code             text,
    series_id        text NOT NULL,              -- LOINC-derived key, or 'local:' || local_key
    group_id         text,
    value_canonical  numeric,
    unit_canonical   text,
    decision_id      text NOT NULL REFERENCES th_coding_decision(decision_id),
    coded_at         timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT th_coding_current_code CHECK ((outcome = 'coded') = (code IS NOT NULL)),
    CONSTRAINT th_coding_current_reason CHECK (outcome = 'coded' OR reason IS NOT NULL)
);
CREATE INDEX IF NOT EXISTS idx_th_coding_current_series ON th_coding_current (series_id, observation_id);
CREATE INDEX IF NOT EXISTS idx_th_coding_current_open ON th_coding_current (outcome) WHERE outcome <> 'coded';

CREATE TABLE IF NOT EXISTS th_coding_history (
    observation_id   bigint NOT NULL REFERENCES th_observation(id) ON DELETE CASCADE,
    coded_at         timestamptz NOT NULL DEFAULT now(),
    cause            text NOT NULL,              -- ingest | amend | recode-release | recode-rules | recode-alias
    release          text NOT NULL,
    outcome          text NOT NULL,
    reason           text,
    code_system      text,
    code             text,
    series_id        text NOT NULL,
    group_id         text,
    value_canonical  numeric,
    unit_canonical   text,
    decision_id      text NOT NULL,
    PRIMARY KEY (observation_id, coded_at)
);

CREATE TABLE IF NOT EXISTS th_coding_alias (
    id            bigserial PRIMARY KEY,
    scope         text NOT NULL,                 -- 'user:<id>' | 'global'
    name_key      text NOT NULL,
    unit_ucum     text NOT NULL DEFAULT '',
    code_system   text,
    code          text,                          -- both NULL: confirmed "not a standard item"
    confirmed_by  text NOT NULL,
    confirmed_at  timestamptz NOT NULL DEFAULT now(),
    note          text,
    CONSTRAINT th_coding_alias_key UNIQUE (scope, name_key, unit_ucum)
);

CREATE TABLE IF NOT EXISTS th_concept (
    release          text NOT NULL,
    code_system      text NOT NULL,
    code             text NOT NULL,
    display          text NOT NULL,              -- LONG_COMMON_NAME (LOINC licence 10.3)
    display_zh       text,
    series_id        text,
    group_id         text,
    loinc_component  text,
    loinc_property   text,
    loinc_time       text,
    loinc_system     text,
    loinc_scale      text,
    loinc_method     text,
    PRIMARY KEY (release, code_system, code)
);

CREATE TABLE IF NOT EXISTS th_series (
    user_id          varchar(200) NOT NULL,
    series_id        text NOT NULL,
    standard         boolean NOT NULL,           -- false when series_id starts with 'local:'
    code_system      text,
    code             text,
    display          text NOT NULL,
    display_zh       text,
    unit_canonical   text,
    kind             text NOT NULL,
    modalities       text[] NOT NULL DEFAULT '{}',
    n                int NOT NULL,
    first_at         timestamptz,
    last_at          timestamptz,
    value_min        numeric,
    value_max        numeric,
    last_value       numeric,
    last_value_text  text,
    reason           text,
    checks_failed    int NOT NULL DEFAULT 0,
    release          text NOT NULL,
    refreshed_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, series_id)
);

CREATE TABLE IF NOT EXISTS th_day_authority (
    user_id         varchar(200) NOT NULL,
    series_id       text NOT NULL,
    local_date      date NOT NULL,
    observation_id  bigint NOT NULL REFERENCES th_observation(id) ON DELETE CASCADE,
    rule            text NOT NULL,
    candidates      int NOT NULL,
    decided_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, series_id, local_date)
);
CREATE INDEX IF NOT EXISTS idx_th_day_authority_obs ON th_day_authority (observation_id);

CREATE TABLE IF NOT EXISTS th_check_result (
    user_id         varchar(200) NOT NULL,
    scope           text NOT NULL,               -- observation | day-cell | series
    observation_id  bigint,
    series_id       text,
    local_date      date,
    rule            text NOT NULL,
    verdict         text NOT NULL,               -- pass | fail | skipped
    detail          jsonb,
    checked_at      timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_th_check_result
    ON th_check_result (user_id, scope, COALESCE(observation_id, 0), COALESCE(series_id, ''),
                        COALESCE(local_date, DATE '0001-01-01'), rule);

-- The one read surface. A row amended by a later row, or entered in error,
-- is not here; readers never filter on status themselves.
CREATE OR REPLACE VIEW v_observation AS
SELECT o.*,
       k.release, k.outcome, k.reason, k.code_system, k.code, k.series_id, k.group_id,
       k.value_canonical, k.unit_canonical, k.decision_id,
       c.display, c.display_zh,
       c.loinc_component, c.loinc_property, c.loinc_time, c.loinc_system, c.loinc_scale, c.loinc_method,
       (a.observation_id IS NOT NULL) AS elected
  FROM th_observation o
  JOIN th_coding_current k ON k.observation_id = o.id
  LEFT JOIN th_concept c ON c.release = k.release AND c.code_system = k.code_system AND c.code = k.code
  LEFT JOIN th_day_authority a ON a.observation_id = o.id
 WHERE o.status = 'final'
   AND NOT EXISTS (SELECT 1 FROM th_observation n WHERE n.amends = o.id);

-- Genotypes: one row per (user, rsID). Not an observation: no time, no unit,
-- no code system of the observation kind. Read by the genetic tool.
CREATE TABLE IF NOT EXISTS th_series_data_genetic (
    id              INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id         character varying(255) COLLATE pg_catalog."default" NOT NULL,
    rsid            character varying(50) COLLATE pg_catalog."default" NOT NULL,
    chromosome      character varying(10) COLLATE pg_catalog."default" NOT NULL,
    "position"      integer NOT NULL,
    genotype        character varying(10) COLLATE pg_catalog."default" NOT NULL,
    create_time     timestamp without time zone NOT NULL DEFAULT now(),
    update_time     timestamp without time zone NOT NULL DEFAULT now(),
    is_deleted      boolean NOT NULL DEFAULT false,
    source_table    character varying(200) COLLATE pg_catalog."default",
    source_table_id character varying(200) COLLATE pg_catalog."default"
);
CREATE INDEX IF NOT EXISTS idx_th_series_data_genetic_rsid    ON th_series_data_genetic USING btree(user_id, rsid);
CREATE INDEX IF NOT EXISTS idx_th_series_data_genetic_user_id ON th_series_data_genetic(user_id);

COMMENT ON TABLE th_series_data_genetic IS 'User genetic data table';
COMMENT ON COLUMN th_series_data_genetic.id IS 'Primary key ID';
COMMENT ON COLUMN th_series_data_genetic.user_id IS 'User ID';
COMMENT ON COLUMN th_series_data_genetic.rsid IS 'Genetic locus ID';
COMMENT ON COLUMN th_series_data_genetic.chromosome IS 'Chromosome';
COMMENT ON COLUMN th_series_data_genetic."position" IS 'Position';
COMMENT ON COLUMN th_series_data_genetic.genotype IS 'Genotype';
COMMENT ON COLUMN th_series_data_genetic.create_time IS 'Creation time';
COMMENT ON COLUMN th_series_data_genetic.update_time IS 'Update time';
COMMENT ON COLUMN th_series_data_genetic.is_deleted IS 'Whether deleted';
