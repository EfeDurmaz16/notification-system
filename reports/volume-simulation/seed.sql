-- Run on the disposable, migrated database described in README.md.
\set ON_ERROR_STOP on
BEGIN;
SET LOCAL TIME ZONE 'UTC';
DO $$
BEGIN
    IF current_database() <> 'notification_volume_sim' THEN
        RAISE EXCEPTION 'Use a separate database named notification_volume_sim';
    END IF;
END $$;

\if :recent
-- Only the recent cohort falls inside the live API window. Leave two minutes
-- of margin at both ends so repeated measurements have the same denominator.
\set first_id 2160000
\set n 7500
SELECT statement_timestamp() - interval '3 minutes' AS first_time,
       interval '8 milliseconds' AS spacing \gset
\else
\set first_id 0
\set n 2160000
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM notifications LIMIT 1) THEN
        RAISE EXCEPTION 'Start with an empty notification history';
    END IF;
END $$;
SELECT statement_timestamp() - interval '24 hours 10 minutes' AS first_time,
       interval '40 milliseconds' AS spacing \gset
\endif

-- Construct the terminal snapshot and its historical transitions together.
-- The normal insert trigger records wall time, which is unsuitable for replay.
ALTER TABLE notifications DISABLE TRIGGER notification_status_created;
CREATE FUNCTION pg_temp.fixture_id(n bigint) RETURNS uuid
    LANGUAGE sql IMMUTABLE STRICT AS $$ SELECT lpad(to_hex(n),32,'0')::uuid $$;

INSERT INTO batches (id, idempotency_key, request_hash, response)
SELECT pg_temp.fixture_id(batch), 'volume-' || batch, ''::bytea, '{}'::jsonb
FROM generate_series(:first_id / 1000, (:first_id + :n - 1) / 1000) batch;

INSERT INTO notifications
    (id, batch_id, item_index, recipient, channel, content, priority, status,
     attempt, message_id, created_at, updated_at)
SELECT pg_temp.fixture_id(id), pg_temp.fixture_id(id / 1000), id % 1000,
       (ARRAY['+12025550123','probe@example.invalid','local-device-token'])[lane],
       (ARRAY['sms','email','push'])[lane], 'Synthetic volume fixture',
       (ARRAY['high','normal','low'])[1 + ((id / 300) % 3)], outcome, 1,
       CASE WHEN outcome='accepted' THEN 'receipt-' || id ELSE '' END,
       completed - interval '2 seconds', completed
FROM (
    SELECT *, CASE WHEN sample < (ARRAY[98,97,95])[lane] THEN 'accepted'
                   WHEN sample < (ARRAY[99,99,98])[lane] THEN 'failed'
                   ELSE 'unknown' END AS outcome
    FROM (
        SELECT :first_id + i AS id, (i % 3) + 1 AS lane, (i / 3) % 100 AS sample,
               :'first_time'::timestamptz + i * :'spacing'::interval AS completed
        FROM generate_series(0, :n - 1) i
    ) generated
) classified;

INSERT INTO status_events (notification_id, attempt, status, changed_at)
SELECT id, 1, h.state, h.changed
FROM notifications
CROSS JOIN LATERAL (VALUES
    ('pending', created_at), ('sending', updated_at - interval '100 milliseconds'),
    (status, updated_at)) AS h(state, changed)
WHERE id >= pg_temp.fixture_id(:first_id) AND id < pg_temp.fixture_id(:first_id + :n);
ALTER TABLE notifications ENABLE TRIGGER notification_status_created;
COMMIT;
