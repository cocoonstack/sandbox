CREATE SEQUENCE IF NOT EXISTS sandboxd_tun_addrs_cursor;
CREATE TABLE IF NOT EXISTS sandboxd_tun_addrs (
    cell         text        NOT NULL,
    address      text        NOT NULL,
    claim        text        NOT NULL,
    allocated_at timestamptz NOT NULL DEFAULT now(),
    released_at  timestamptz,
    PRIMARY KEY (cell, address)
);
CREATE UNIQUE INDEX IF NOT EXISTS sandboxd_tun_addrs_claim ON sandboxd_tun_addrs (cell, claim) WHERE released_at IS NULL;
