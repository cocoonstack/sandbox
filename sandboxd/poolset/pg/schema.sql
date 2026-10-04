CREATE TABLE IF NOT EXISTS sandboxd_pool_sets (
    cell        text        PRIMARY KEY,
    config_seed text        NOT NULL DEFAULT '',
    pools       jsonb       NOT NULL,
    version     bigint      NOT NULL CHECK (version > 0),
    updated_at  timestamptz NOT NULL DEFAULT now()
);
