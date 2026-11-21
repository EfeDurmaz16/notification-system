-- Supply previously observed Debezium event header IDs as a comma-separated list.
-- Both modes record scan progress. Only apply=true deletes eligible records.
\set ON_ERROR_STOP on
\if :{?apply}
\else
\set apply false
\endif
\if :{?published_ids}
\else
\set published_ids ''
\endif
\if :{?keep_days}
\if :{?before}
DO $$ BEGIN RAISE EXCEPTION 'Supply either keep_days or before'; END $$;
\endif
-- make_interval validates the integer and leaves calendar arithmetic to PostgreSQL.
SELECT CASE WHEN :'keep_days'::int >= 1
            THEN now() - make_interval(days => :'keep_days'::int)
            ELSE 'infinity'::timestamptz END AS before \gset
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
-- Serialize jobs in the same mode. Freeze the upper bound for this sweep so
-- continuous admissions cannot prevent a later revisit of unresolved histories.
SELECT last_changed_at AS last_time, last_seq, least(coalesce(sweep_before, cutoff), cutoff) AS scan_before
FROM retention_cursors CROSS JOIN retention_options
WHERE applying=:'apply'::boolean FOR UPDATE OF retention_cursors \gset

-- Seek in status_events_retention and bound work before testing eligibility.
CREATE TEMP TABLE retention_scan ON COMMIT DROP AS
SELECT changed_at, seq, notification_id FROM status_events
WHERE (changed_at,seq) > (:'last_time'::timestamptz, :'last_seq'::bigint)
  AND changed_at < :'scan_before'::timestamptz
ORDER BY changed_at,seq LIMIT 1000;
CREATE TEMP TABLE expired_events ON COMMIT DROP AS
SELECT e.seq FROM retention_scan e
JOIN LATERAL (SELECT status,updated_at FROM notifications WHERE id=e.notification_id LIMIT 1) n ON true
WHERE n.status IN ('accepted','failed','cancelled') AND n.updated_at < :'scan_before'::timestamptz;

SELECT (SELECT count(DISTINCT id) FROM expired_outbox) AS eligible_outbox_rows,
       (SELECT count(*) FROM retention_scan) AS examined_audit_events,
       (SELECT count(*) FROM expired_events) AS eligible_audit_events;
\if :apply
DELETE FROM outbox o USING expired_outbox e WHERE o.id=e.id;
DELETE FROM status_events s USING expired_events e WHERE s.seq=e.seq;
\endif
-- An incomplete page finishes this sweep. Start again from the beginning on
-- the next pass, including older events whose notification has since resolved.
WITH boundary AS (
    SELECT changed_at,seq FROM retention_scan ORDER BY changed_at,seq OFFSET 999 LIMIT 1
)
UPDATE retention_cursors SET
    last_changed_at=coalesce((SELECT changed_at FROM boundary), '-infinity'),
    last_seq=coalesce((SELECT seq FROM boundary), 0),
    sweep_before=(SELECT :'scan_before'::timestamptz FROM boundary)
WHERE applying=:'apply'::boolean;
COMMIT;
