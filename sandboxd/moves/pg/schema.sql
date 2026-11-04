CREATE SEQUENCE IF NOT EXISTS sandboxd_moves_epoch;
CREATE TABLE IF NOT EXISTS sandboxd_moves (
    cell       text        NOT NULL,
    id         text        NOT NULL,
    from_node  text        NOT NULL,
    to_node    text        NOT NULL,
    state      text        NOT NULL CHECK (state IN ('pending', 'final', 'committed', 'aborted')),
    epoch      bigint      NOT NULL CHECK (epoch > 0),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (cell, id)
);
