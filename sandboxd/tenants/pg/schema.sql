CREATE TABLE IF NOT EXISTS sandboxd_tenants (
    name         text        PRIMARY KEY,
    token_sha256 bytea       NOT NULL UNIQUE CHECK (length(token_sha256) = 32),
    max_claims   integer     NOT NULL DEFAULT 0 CHECK (max_claims >= 0),
    egress_class text        NOT NULL DEFAULT '',
    updated_at   timestamptz NOT NULL DEFAULT now()
);
