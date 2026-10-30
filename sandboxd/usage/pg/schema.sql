CREATE TABLE IF NOT EXISTS sandboxd_usage (
    cell        text        NOT NULL,
    cursor      bigint      GENERATED ALWAYS AS IDENTITY,
    node        text        NOT NULL,
    seq         bigint      NOT NULL CHECK (seq > 0),
    line_sha256 bytea       NOT NULL,
    t           timestamptz NOT NULL,
    ev          text        NOT NULL,
    id          text        NOT NULL,
    vm          text        NOT NULL DEFAULT '',
    key         text        NOT NULL DEFAULT '',
    tenant      text        NOT NULL DEFAULT '',
    volumes     text[],
    volumes_rw  text[],
    children    text[],
    ref         text        NOT NULL DEFAULT '',
    upstream    text        NOT NULL DEFAULT '',
    secret      text        NOT NULL DEFAULT '',
    tx          bigint      NOT NULL DEFAULT 0,
    rx          bigint      NOT NULL DEFAULT 0,
    PRIMARY KEY (cell, cursor),
    UNIQUE (cell, node, seq)
);
CREATE INDEX IF NOT EXISTS sandboxd_usage_tenant ON sandboxd_usage (cell, tenant, cursor);
CREATE INDEX IF NOT EXISTS sandboxd_usage_t ON sandboxd_usage (cell, t);
