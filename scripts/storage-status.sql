-- Read-only estimates for capacity planning. pg_class counts reflect the last
-- vacuum or analyze, and pg_total_relation_size includes indexes and TOAST.
\set ON_ERROR_STOP on
SELECT relname AS relation, reltuples::bigint AS estimated_rows,
       pg_total_relation_size(oid) AS total_bytes
FROM pg_class
WHERE oid IN ('notifications'::regclass, 'batches'::regclass,
              'outbox'::regclass, 'status_events'::regclass)
ORDER BY total_bytes DESC;

SELECT slot_name, active, wal_status,
       pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) AS retained_wal_bytes,
       pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) AS unconfirmed_wal_bytes
FROM pg_replication_slots WHERE slot_name = 'notify_outbox';
