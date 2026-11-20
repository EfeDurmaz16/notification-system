-- Run after a measured experiment. No writes and no application clocks.
-- psql -X -q -v ON_ERROR_STOP=1 -v run_id=... -f scripts/latency-breakdown.sql
-- Keys use the harness's "<opaque run ID>-<numeric offset>" format.
\set ON_ERROR_STOP on
\pset format unaligned
\pset fieldsep '\t'
\pset footer off
BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout = '30s';
WITH selected AS MATERIALIZED (
    SELECT n.id, n.batch_id, n.priority, n.status
    FROM batches b
    JOIN notifications n ON n.batch_id = b.id
    WHERE starts_with(b.idempotency_key, :'run_id' || '-')
      AND substr(b.idempotency_key, length(:'run_id') + 2) ~ '^[0-9]+$'
), boundaries AS (
    SELECT s.id, s.batch_id, s.priority, s.status,
           min(e.changed_at) FILTER (WHERE e.status='pending' AND e.attempt=1) AS queued,
           min(e.changed_at) FILTER (WHERE e.status='sending' AND e.attempt=1) AS sending,
           min(e.changed_at) FILTER (WHERE e.status='accepted' AND e.attempt=1) AS accepted
    FROM selected s LEFT JOIN status_events e ON e.notification_id=s.id
    GROUP BY s.id,s.batch_id,s.priority,s.status
), stages AS (
    SELECT b.*,v.stage,v.duration
    FROM boundaries b
    CROSS JOIN LATERAL (VALUES
        ('1_pending_to_sending', b.sending-b.queued),
        ('2_sending_to_accepted', b.accepted-b.sending),
        ('3_pending_to_accepted', b.accepted-b.queued)
    ) AS v(stage,duration)
)
SELECT coalesce(priority,'all') AS priority,stage,
       count(DISTINCT batch_id) AS batches,count(*) AS notifications,
       count(*) FILTER (WHERE status='accepted') AS accepted_at_query,
       count(duration) AS samples,
       round((percentile_cont(0.50) WITHIN GROUP (ORDER BY extract(epoch FROM duration)*1000))::numeric,2) AS p50_ms,
       round((percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM duration)*1000))::numeric,2) AS p95_ms,
       round((percentile_cont(0.99) WITHIN GROUP (ORDER BY extract(epoch FROM duration)*1000))::numeric,2) AS p99_ms
FROM stages
GROUP BY GROUPING SETS ((priority,stage),(stage))
ORDER BY GROUPING(priority) DESC,priority,stage;
COMMIT;
