-- 32_genomics.sql: one active genotype set per person, with atomic visibility.
--
-- A loading or failed set is never queried. Activation and supersession happen
-- in one transaction, so a partly imported genome cannot replace a good one.

CREATE TABLE IF NOT EXISTS th_genotype_set (
    id                  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id             varchar(255) NOT NULL,
    file_key            varchar(255),
    format_id           varchar(40) NOT NULL,
    vendor              varchar(40),
    chip_fingerprint    varchar(64),
    build_declared      varchar(10),
    build_detected      varchar(10) NOT NULL DEFAULT 'unknown',
    n_rows              integer NOT NULL DEFAULT 0,
    n_called            integer NOT NULL DEFAULT 0,
    sex_inferred        varchar(10) NOT NULL DEFAULT 'unknown',
    status              varchar(12) NOT NULL DEFAULT 'loading',
    normalizer_version  varchar(20) NOT NULL DEFAULT 'pending',
    site_table_version  varchar(40) NOT NULL DEFAULT 'pending',
    created_at          timestamptz NOT NULL DEFAULT now(),
    activated_at        timestamptz,
    superseded_at       timestamptz,
    CONSTRAINT th_genotype_set_status CHECK (status IN ('loading', 'active', 'superseded', 'failed')),
    CONSTRAINT th_genotype_set_sex CHECK (sex_inferred IN ('female', 'male', 'unknown')),
    CONSTRAINT th_genotype_set_counts CHECK (n_rows >= 0 AND n_called >= 0 AND n_called <= n_rows)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_th_genotype_set_active_user
    ON th_genotype_set (user_id) WHERE status = 'active';
CREATE INDEX IF NOT EXISTS idx_th_genotype_set_user_created
    ON th_genotype_set (user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_th_genotype_set_file
    ON th_genotype_set (user_id, file_key) WHERE file_key IS NOT NULL;

CREATE TABLE IF NOT EXISTS th_genotype (
    set_id          bigint NOT NULL REFERENCES th_genotype_set(id) ON DELETE CASCADE,
    rsid            varchar(50) NOT NULL,
    rsid_raw        varchar(50) NOT NULL,
    chrom           varchar(3) NOT NULL,
    position_raw    integer NOT NULL,
    pos37           integer,
    pos38           integer,
    ref             text,
    alt             text,
    gene            varchar(32),
    genotype_raw    varchar(100) NOT NULL,
    gt              varchar(15),
    call_status     varchar(20) NOT NULL,
    zygosity        varchar(20),
    strand_check    varchar(24),
    PRIMARY KEY (set_id, rsid),
    CONSTRAINT th_genotype_call_status CHECK (
        call_status IN ('called', 'no_call', 'not_applicable', 'unresolved')
    )
);

CREATE INDEX IF NOT EXISTS idx_th_genotype_location
    ON th_genotype (set_id, chrom, pos38);
CREATE INDEX IF NOT EXISTS idx_th_genotype_gene
    ON th_genotype (set_id, gene) WHERE gene IS NOT NULL;

CREATE TABLE IF NOT EXISTS th_pgx_result (
    set_id              bigint NOT NULL REFERENCES th_genotype_set(id) ON DELETE CASCADE,
    gene                varchar(32) NOT NULL,
    diplotype           varchar(40),
    phenotype           varchar(80),
    activity_score      numeric,
    callable            boolean NOT NULL,
    missing_positions   text[] NOT NULL DEFAULT '{}',
    knowledge_version   varchar(40) NOT NULL,
    created_at          timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (set_id, gene, knowledge_version)
);
