
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


CREATE TABLE IF NOT EXISTS health_vital_webhook (
    id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    create_at timestamp without time zone not null default CURRENT_TIMESTAMP,
    update_at timestamp without time zone not null default CURRENT_TIMESTAMP,
    is_del boolean not null,
    event_type character varying not null default ''::character varying,
    app_user_id bigint not null default 0,
    user_id character varying not null default ''::character varying,
    client_user_id character varying not null default ''::character varying,
    team_id character varying not null default ''::character varying,
    msg_id character varying not null default ''::character varying,
    req_id character varying not null default ''::character varying,
    status integer not null default 0,
    doc character varying not null default ''::character varying
);
CREATE INDEX IF NOT EXISTS idx_health_vital_webhook_app_user_id ON health_vital_webhook(app_user_id);
CREATE INDEX IF NOT EXISTS idx_health_vital_webhook_user_id ON health_vital_webhook(user_id);


CREATE TABLE IF NOT EXISTS health_data_epic (
    id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    create_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    is_del boolean NOT NULL DEFAULT false,
    msg_id character varying(255) UNIQUE,
    raw_data jsonb NOT NULL,
    theta_user_id character varying(100),
    patient_id character varying(255),
    clinic_id character varying(255),
    resource_type character varying(100)
);
CREATE INDEX IF NOT EXISTS idx_health_data_epic_theta_user_id ON health_data_epic(theta_user_id);
CREATE INDEX IF NOT EXISTS idx_health_data_epic_clinic_id ON health_data_epic(clinic_id);


CREATE TABLE IF NOT EXISTS health_data_oracle (
    id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    create_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    is_del boolean NOT NULL DEFAULT false,
    msg_id character varying(255) UNIQUE,
    raw_data jsonb NOT NULL,
    theta_user_id character varying(100),
    patient_id character varying(255),
    clinic_id character varying(255),
    resource_type character varying(100)
);
CREATE INDEX IF NOT EXISTS idx_health_data_oracle_theta_user_id ON health_data_oracle(theta_user_id);
CREATE INDEX IF NOT EXISTS idx_health_data_oracle_clinic_id ON health_data_oracle(clinic_id);


CREATE TABLE IF NOT EXISTS health_data_libre
(
    id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    create_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    user_id character varying(128) NOT NULL,
    out_uid character varying(128) NOT NULL DEFAULT '',
    key bigint NOT NULL,
    data text NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_health_data_libre_key ON health_data_libre(key);
