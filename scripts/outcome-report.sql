-- Count terminal notifications in UTC calendar buckets and over the whole range.
\set ON_ERROR_STOP on
BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL TIME ZONE 'UTC';
\if :{?bucket}
\else
\set bucket hour
\endif
\if :{?to}
\else
\set to now
\endif
\if :{?from}
\else
SELECT :'to'::timestamptz - interval '5 hours' AS "from" \gset
\endif

SELECT CASE WHEN :'bucket' IN ('hour','day','week')
                 AND isfinite(:'from'::timestamptz) AND isfinite(:'to'::timestamptz)
                 AND :'from'::timestamptz < :'to'::timestamptz
            THEN :'to'::timestamptz - date_trunc(:'bucket', :'from'::timestamptz)
                 <= ('1 ' || :'bucket')::interval * 10000
            ELSE false END AS valid_range \gset
\if :valid_range
\else
DO $$ BEGIN RAISE EXCEPTION 'Use hour, day, or week and finite from < to spanning at most 10000 calendar buckets'; END $$;
\endif

WITH buckets AS (
    SELECT calendar_start,
           greatest(calendar_start, :'from'::timestamptz) AS starts_at,
           least(calendar_start + ('1 ' || :'bucket')::interval, :'to'::timestamptz) AS ends_at
    FROM generate_series(date_trunc(:'bucket', :'from'::timestamptz), :'to'::timestamptz,
                         ('1 ' || :'bucket')::interval) AS times(calendar_start)
    WHERE calendar_start < :'to'::timestamptz
), outcomes AS (
    SELECT date_trunc(:'bucket', updated_at) AS calendar_start, channel,
           count(*) FILTER (WHERE status = 'accepted') AS accepted,
           count(*) FILTER (WHERE status = 'failed') AS failed,
           count(*) FILTER (WHERE status = 'unknown') AS unknown
    FROM notifications
    WHERE updated_at >= :'from'::timestamptz AND updated_at < :'to'::timestamptz
      AND status IN ('accepted','failed','unknown')
    GROUP BY 1, channel
)
SELECT CASE WHEN grouping(b.starts_at) = 1 THEN 'total' ELSE 'bucket' END AS row_kind,
       coalesce(b.starts_at, :'from'::timestamptz) AS from_utc,
       coalesce(b.ends_at, :'to'::timestamptz) AS to_utc, c.channel,
       coalesce(sum(o.accepted), 0) AS accepted,
       coalesce(sum(o.failed), 0) AS failed,
       coalesce(sum(o.unknown), 0) AS unknown
FROM buckets b CROSS JOIN (VALUES ('sms'), ('email'), ('push')) AS c(channel)
LEFT JOIN outcomes o ON o.calendar_start = b.calendar_start AND o.channel = c.channel
GROUP BY GROUPING SETS ((b.starts_at, b.ends_at, c.channel), (c.channel))
ORDER BY grouping(b.starts_at), from_utc, c.channel;
COMMIT;
