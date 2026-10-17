CREATE TABLE IF NOT EXISTS sandboxd_nodes (
    cell        text        NOT NULL,
    node        text        NOT NULL,
    lease_until timestamptz NOT NULL,
    PRIMARY KEY (cell, node)
);
