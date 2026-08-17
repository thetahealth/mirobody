CREATE TABLE IF NOT EXISTS health_data_oura (
    id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    create_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    is_del boolean NOT NULL DEFAULT false,
    msg_id character varying(200),
    raw_data jsonb NOT NULL,
    theta_user_id character varying(100),
    external_user_id character varying(100)
);

CREATE INDEX IF NOT EXISTS idx_health_data_oura_theta_user_id ON health_data_oura(theta_user_id);
