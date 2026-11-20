-- psql -v run_id=<raw report runId> -f stages.sql
\set ON_ERROR_STOP on
WITH cohort AS MATERIALIZED (
    SELECT n.priority, n.created_at, n.first_attempt_at AS claimed, n.updated_at AS accepted
    FROM notifications n JOIN batches b ON n.batch_id = b.id
    WHERE starts_with(b.idempotency_key, :'run_id' || '-')
      AND substr(b.idempotency_key, length(:'run_id') + 2) ~ '^[0-9]+$'
), anchor AS (
    SELECT min(claimed) AS first_claim FROM cohort
)
SELECT priority, count(*) AS population,
       count(*) FILTER (WHERE claimed < first_claim + interval '30 seconds') AS early_claims,
       percentile_disc(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM claimed - created_at)) AS queue_p95_seconds,
       percentile_disc(0.95) WITHIN GROUP (ORDER BY 1000 * extract(epoch FROM accepted - claimed)) AS send_p95_ms
FROM cohort CROSS JOIN anchor
GROUP BY priority ORDER BY priority;
