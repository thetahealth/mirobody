-- Medications are an ENTITY, not a time series.
--
-- The three tables here exist because a medication does not fit
-- `th_series_data` and never did: a plan has a schedule (a tuple of dosing
-- instructions), a lifecycle (active → stopped → resumed as a new course) and
-- a code system of its own (RxNorm), while a reading has a value, a unit and
-- an instant. Storing "takes metformin 500 mg twice a day" as a reading forces
-- a choice between losing the schedule and inventing a fake value — and makes
-- every aggregation query step over rows that are not measurements.
--
-- `mirobody.kernel.meds` is the vocabulary these tables persist; the column names are
-- its field names so a reader can move between them without a translation
-- table. Nothing derived is stored: `due`, `missed`, `upcoming` and
-- `unschedulable` come from `meds.slot_state(now, grace)` at read time, and
-- adherence from `meds.adherence`. A stored `missed` is a lie the moment the
-- person opens the app and marks the dose taken.
--
-- PHI: the drug name, the strength and the person's reason for skipping a dose
-- are free-text health data and go through `encrypt_content`, exactly as
-- `th_series_data.comment` does. `concept_key` is a code list or a hash and is
-- safe to log, join on and index — which is why it, not the name, is the key
-- every other table references.

CREATE TABLE IF NOT EXISTS th_medication_plan (
    plan_id           varchar(200) PRIMARY KEY,
    user_id           varchar(200) NOT NULL,
    concept_key       varchar(200) NOT NULL,          -- rxnorm:<codes> | text:<hash> — never a name
    concept_text      text,                           -- ENCRYPTED: the name as the person wrote it
    concept_strength  text,                           -- ENCRYPTED
    concept_form      varchar(100) DEFAULT '',
    codes             jsonb DEFAULT '[]'::jsonb,      -- [{system, code, display, tty}]
    schedule          jsonb NOT NULL DEFAULT '[]'::jsonb,
    start_date        date NOT NULL,
    end_date          date,
    status            varchar(32) NOT NULL DEFAULT 'active',   -- active | stopped | entered_in_error
    classification    varchar(100) DEFAULT '',
    confirmed         boolean NOT NULL DEFAULT true,
    order_id          varchar(200) DEFAULT '',
    source            varchar(100) DEFAULT '',
    source_record_id  varchar(200) DEFAULT '',
    stopped_on        date,
    create_time       timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    update_time       timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    deleted           integer NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_th_medication_plan_user
    ON th_medication_plan (user_id, status) WHERE deleted = 0;
CREATE INDEX IF NOT EXISTS idx_th_medication_plan_concept
    ON th_medication_plan (user_id, concept_key) WHERE deleted = 0;

-- A period during which a plan was followed (OMOP `drug_exposure`). Closed by
-- a stop, a supersede or the plan's own end; an OPEN course has end_date NULL
-- and whoever exports it materialises it at today in the subject's zone.
CREATE TABLE IF NOT EXISTS th_medication_course (
    course_id    varchar(200) PRIMARY KEY,
    plan_id      varchar(200) NOT NULL,
    user_id      varchar(200) NOT NULL,
    order_id     varchar(200) DEFAULT '',
    start_date   date NOT NULL,
    end_date     date,
    closed_by    varchar(32),                          -- stopped | superseded | completed | NULL while open
    create_time  timestamp with time zone DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_th_medication_course_plan
    ON th_medication_course (plan_id, start_date);

-- What actually happened. Only `taken` and `skipped` are ever stored — the
-- other four states of a dose are derived from the clock.
CREATE TABLE IF NOT EXISTS th_dose_event (
    event_id     varchar(200) PRIMARY KEY,
    plan_id      varchar(200) NOT NULL,
    user_id      varchar(200) NOT NULL,
    status       varchar(32) NOT NULL,                 -- taken | skipped
    taken_at_ms  bigint NOT NULL,
    tz           varchar(64) NOT NULL DEFAULT 'UTC',
    local_date   date NOT NULL,                        -- the event's day in `tz`, for the log view
    slot_date    date,                                 -- the three parts of DoseSlot.key, or NULL
    slot_name    varchar(64),                          --   for an unscheduled intake
    dose_value   double precision,
    dose_unit    varchar(32) DEFAULT '',
    recorded_by  varchar(32) NOT NULL DEFAULT 'user',  -- user | caregiver | device | import
    reason       text,                                 -- ENCRYPTED: the person's words
    create_time  timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    deleted      integer NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_th_dose_event_user_day
    ON th_dose_event (user_id, local_date) WHERE deleted = 0;
CREATE INDEX IF NOT EXISTS idx_th_dose_event_slot
    ON th_dose_event (plan_id, slot_date, slot_name) WHERE deleted = 0;

-- A correction is a LAYER, never an edit. The original row is never rewritten:
-- a reader replays the overrides for a target on top of it (`mirobody.kernel.overlay`),
-- and a deletion is an override too (`tombstone`). That is what makes "who
-- changed this, when, and what did it say before" answerable at all.
CREATE TABLE IF NOT EXISTS th_override (
    override_id  varchar(200) PRIMARY KEY,
    user_id      varchar(200) NOT NULL,
    target_kind  varchar(64) NOT NULL,                 -- medication_plan | dose_event | reading
    target_id    varchar(200) NOT NULL,
    field        varchar(100) NOT NULL,                -- 'deleted' is the tombstone
    value        text,                                 -- ENCRYPTED: JSON, whatever the field holds
    actor        varchar(200) NOT NULL DEFAULT '',     -- opaque: "user:…", "reviewer:…", "system"
    at_ms        bigint NOT NULL,
    seq          integer NOT NULL DEFAULT 0,           -- ties between two edits in one millisecond
    reason       text,                                 -- ENCRYPTED
    create_time  timestamp with time zone DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_th_override_target
    ON th_override (target_kind, target_id, at_ms, seq);
CREATE INDEX IF NOT EXISTS idx_th_override_user
    ON th_override (user_id, at_ms);
