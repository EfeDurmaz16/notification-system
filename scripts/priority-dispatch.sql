-- Read-only per-class send claims for a completed loadtest run.
-- All windows start at the first durable send claim, and use [start,end).
\set ON_ERROR_STOP on
\pset format unaligned
\pset fieldsep '\t'
\pset footer off
BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout = '30s';
WITH cohort AS MATERIALIZED (
    SELECT n.id, n.priority
    FROM batches b JOIN notifications n ON n.batch_id=b.id
    WHERE starts_with(b.idempotency_key, :'run_id' || '-')
      AND substr(b.idempotency_key, length(:'run_id') + 2) ~ '^[0-9]+$'
), first_events AS MATERIALIZED (
    SELECT c.id, c.priority,
           min(e.changed_at) FILTER (WHERE e.status='pending') AS queued,
           min(e.changed_at) FILTER (WHERE e.status='sending') AS claimed
    FROM cohort c LEFT JOIN status_events e ON e.notification_id=c.id AND e.attempt=1
    GROUP BY c.id,c.priority
), anchor AS (
    SELECT min(claimed) AS first_claim FROM first_events
), windows(lo,hi) AS (
    VALUES (0,10),(10,20),(20,30),(0,30)
)
SELECT lo AS start_seconds,hi AS end_seconds, priority,
       count(*) FILTER (WHERE queued<=first_claim+lo*interval '1 second'
                        AND (claimed IS NULL OR claimed>=first_claim+lo*interval '1 second')) AS outstanding_at_start,
       count(*) FILTER (WHERE claimed>=first_claim+lo*interval '1 second'
                        AND claimed<first_claim+hi*interval '1 second') AS claims,
       count(*) FILTER (WHERE queued<first_claim+hi*interval '1 second'
                        AND (claimed IS NULL OR claimed>=first_claim+hi*interval '1 second')) AS outstanding_at_end
FROM first_events CROSS JOIN windows CROSS JOIN anchor
GROUP BY lo,hi,priority ORDER BY hi,lo,priority;
COMMIT;
