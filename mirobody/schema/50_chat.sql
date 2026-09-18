-- 50_chat.sql: conversations.
--
--   th_sessions        one conversation; `query_user_id` when it is about
--                      someone else in the care circle
--   th_messages        its turns; `content` is encrypted (encrypt_content)
--   v_th_messages      rated assistant turns, decrypted, for review
--   th_session_share   a public share link for one session

CREATE TABLE IF NOT EXISTS th_messages (
    id                varchar(50) UNIQUE NOT NULL,
    user_id           character varying(100) NOT NULL,
    user_name         character varying(100),
    session_id        character varying(100) NOT NULL,
    role              character varying(20) NOT NULL,
    content           text NOT NULL,
    reasoning         text,
    agent             character varying(50),
    provider          character varying(50),
    input_prompt      text,
    question_id       character varying(50),
    rating            integer,
    created_at        timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    message_type      text,
    is_del            boolean NOT NULL DEFAULT false,
    updated_at        timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    group_id          VARCHAR(64),
    scene             VARCHAR(32) DEFAULT 'web',
    query_user_id     VARCHAR(100),
    reference_task_id VARCHAR(128) DEFAULT NULL
);
-- Two indexes only: every remaining query on this table filters by session
-- or by question. The file-list and trigram indexes it used to carry are
-- dropped in 90_retire.sql.
CREATE INDEX IF NOT EXISTS idx_th_message_sessionID  ON th_messages(session_id);
CREATE INDEX IF NOT EXISTS idx_th_message_questionID ON th_messages(question_id);

CREATE TABLE IF NOT EXISTS th_sessions (
    session_id    varchar(100) UNIQUE NOT NULL,
    user_id       character varying(100),
    user_name     character varying(100),
    query_user_id VARCHAR(100),
    in_use        BOOLEAN DEFAULT TRUE,
    summary       text,
    created_at    timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    category      VARCHAR(50),                -- food, report, medicine, rtc, journal, other
    preview       VARCHAR(200)                -- conversation preview snippet
);
ALTER TABLE th_sessions ADD COLUMN IF NOT EXISTS category VARCHAR(50);
ALTER TABLE th_sessions ADD COLUMN IF NOT EXISTS preview VARCHAR(200);
CREATE INDEX IF NOT EXISTS idx_th_sessions_user_id  ON th_sessions(user_id);
CREATE INDEX IF NOT EXISTS idx_th_sessions_category ON th_sessions(category);

COMMENT ON COLUMN th_sessions.category IS 'File category: food, report, medicine, rtc, journal, other';

DROP VIEW IF EXISTS v_th_messages;
CREATE OR REPLACE VIEW v_th_messages AS
SELECT
    id,
    user_id,
    user_name,
    session_id,
    role,
    decrypt_content(content) AS content,
    reasoning,
    agent,
    provider,
    input_prompt,
    question_id,
    rating,
    created_at
FROM th_messages
WHERE role='assistant' AND rating IS NOT NULL;

CREATE TABLE IF NOT EXISTS th_session_share (
    share_session_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id       VARCHAR(100) NOT NULL UNIQUE,
    user_id          VARCHAR(100) NOT NULL,
    created_at       TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at       TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    is_active        BOOLEAN DEFAULT TRUE
);
CREATE INDEX IF NOT EXISTS idx_th_session_share_session_id ON th_session_share(session_id);
CREATE INDEX IF NOT EXISTS idx_th_session_share_user_id    ON th_session_share(user_id);
