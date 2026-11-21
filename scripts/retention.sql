-- Supply previously observed Debezium event header IDs as a comma-separated list.
-- A transaction previews by default. Set apply=true to commit one bounded chunk.
\set ON_ERROR_STOP on
\if :{?apply}
\else
\set apply false
\endif
\if :{?published_ids}
\else
\set published_ids ''
\endif
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '15s';
CREATE TEMP TABLE retention_options ON COMMIT DROP AS
SELECT :'before'::timestamptz AS cutoff, string_to_array(:'published_ids', ',')::uuid[] AS published;
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM retention_options WHERE NOT isfinite(cutoff) OR cutoff >= now() OR cardinality(published)>1000) THEN
        RAISE EXCEPTION 'Supply a finite past cutoff and at most 1000 published outbox event IDs';
    END IF;
END $$;
-- Terminal state alone is insufficient evidence that a given outbox row reached Kafka.
CREATE TEMP TABLE expired_outbox ON COMMIT DROP AS
SELECT o.id FROM retention_options r CROSS JOIN unnest(r.published) AS p(id)
JOIN outbox o ON o.id=p.id JOIN notifications n ON n.id=o.aggregateid
WHERE o.created_at < r.cutoff AND n.updated_at < r.cutoff AND n.status IN ('accepted','failed','cancelled');
-- Keep unknown outcomes and their entire history for investigation.
CREATE TEMP TABLE expired_events ON COMMIT DROP AS
SELECT e.seq FROM status_events e JOIN notifications n ON n.id=e.notification_id CROSS JOIN retention_options r
WHERE n.status IN ('accepted','failed','cancelled') AND n.updated_at < r.cutoff AND e.changed_at < r.cutoff
ORDER BY e.changed_at,e.seq LIMIT 1000;
SELECT (SELECT count(DISTINCT id) FROM expired_outbox) AS eligible_outbox_rows,
       (SELECT count(*) FROM expired_events) AS eligible_audit_events;
\if :apply
DELETE FROM outbox o USING expired_outbox e WHERE o.id=e.id;
DELETE FROM status_events s USING expired_events e WHERE s.seq=e.seq;
COMMIT;
\else
ROLLBACK;
\endif
