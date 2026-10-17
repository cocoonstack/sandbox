CREATE SEQUENCE IF NOT EXISTS sandboxd_archives_epoch;
CREATE TABLE IF NOT EXISTS sandboxd_archives (
    cell         text        NOT NULL,
    id           text        NOT NULL,
    tenant       text        NOT NULL DEFAULT '',
    token_sha256 text        NOT NULL,
    owner        text        NOT NULL,
    epoch        bigint      NOT NULL CHECK (epoch > 0),
    consumed     boolean     NOT NULL DEFAULT false,
    checkpoint   text        NOT NULL,
    deadline     timestamptz,
    claim        jsonb       NOT NULL,
    updated_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (cell, id)
);
CREATE INDEX IF NOT EXISTS sandboxd_archives_deadline ON sandboxd_archives (cell, deadline) WHERE deadline IS NOT NULL;
CREATE INDEX IF NOT EXISTS sandboxd_archives_consumed ON sandboxd_archives (cell) WHERE consumed;
