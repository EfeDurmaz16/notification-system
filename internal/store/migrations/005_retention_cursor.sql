-- Independent circular sweeps let operators preview without advancing deletion.
CREATE TABLE retention_cursors (
    applying boolean PRIMARY KEY,
    last_changed_at timestamptz NOT NULL DEFAULT '-infinity',
    last_seq bigint NOT NULL DEFAULT 0,
    sweep_before timestamptz
);
INSERT INTO retention_cursors (applying) VALUES (false), (true);
