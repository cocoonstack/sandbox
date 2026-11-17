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
CREATE TABLE IF NOT EXISTS sandboxd_nodes (
    cell        text        NOT NULL,
    node        text        NOT NULL,
    lease_until timestamptz NOT NULL,
    PRIMARY KEY (cell, node)
);
