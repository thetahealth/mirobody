
CREATE TABLE IF NOT EXISTS health_vital_user (
    id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    create_at timestamp without time zone not null default CURRENT_TIMESTAMP,
    update_at timestamp without time zone not null default CURRENT_TIMESTAMP,
    is_del boolean not null,
    app_user_id character varying(100) not null default 0,
    vital_user_id character varying not null default ''::character varying
);
CREATE INDEX IF NOT EXISTS idx_health_vital_user_app_user_id   ON health_vital_user(app_user_id);
CREATE INDEX IF NOT EXISTS idx_health_vital_user_vital_user_id ON health_vital_user(vital_user_id);





