-- One bounded set of counters, striped by notification ID to spread write locks.
CREATE TABLE notification_metric_totals (
    stripe smallint NOT NULL,
    status text NOT NULL,
    bucket smallint NOT NULL,
    quantity bigint NOT NULL,
    latency_seconds numeric NOT NULL,
    PRIMARY KEY (stripe, status, bucket)
);
CREATE INDEX notifications_recent_outcomes ON notifications (updated_at) INCLUDE (channel,status)
    WHERE status IN ('accepted','failed','unknown');

-- The same bucket boundaries apply to existing rows and future transitions.
CREATE FUNCTION notification_metric_bucket(state text, elapsed interval) RETURNS smallint
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$ SELECT CASE
    WHEN state <> 'accepted' THEN 0
    WHEN elapsed <= interval '100 milliseconds' THEN 1
    WHEN elapsed <= interval '500 milliseconds' THEN 2
    WHEN elapsed <= interval '1 second' THEN 3
    WHEN elapsed <= interval '5 seconds' THEN 4
    WHEN elapsed <= interval '10 seconds' THEN 5
    WHEN elapsed <= interval '60 seconds' THEN 6
    ELSE 7 END::smallint $$;

CREATE FUNCTION update_notification_metrics() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    changes text;
BEGIN
    IF TG_OP = 'TRUNCATE' THEN
        TRUNCATE notification_metric_totals;
        RETURN NULL;
    END IF;
    IF TG_OP = 'INSERT' THEN
        changes := 'SELECT id,status,updated_at-created_at AS elapsed,1 AS sign FROM added';
    ELSIF TG_OP = 'DELETE' THEN
        changes := 'SELECT id,status,updated_at-created_at AS elapsed,-1 AS sign FROM removed';
    ELSE
        changes := 'SELECT id,status,updated_at-created_at AS elapsed,1 AS sign FROM added UNION ALL
                    SELECT id,status,updated_at-created_at AS elapsed,-1 AS sign FROM removed';
    END IF;
    -- The transition tables contain only this statement. Acquire all counter
    -- locks in key order, including for batch admissions and scheduler updates.
    EXECUTE 'INSERT INTO notification_metric_totals AS totals
        SELECT get_byte(uuid_send(id),0) % 32, status, notification_metric_bucket(status,elapsed),
               sum(sign), sum(CASE WHEN status=''accepted'' THEN sign*extract(epoch FROM elapsed) ELSE 0 END)
        FROM (' || changes || ') AS delta
        GROUP BY 1,2,3 ORDER BY 1,2,3
        ON CONFLICT (stripe,status,bucket) DO UPDATE SET quantity=totals.quantity+excluded.quantity,
               latency_seconds=totals.latency_seconds+excluded.latency_seconds';
    RETURN NULL;
END; $$;
CREATE TRIGGER notification_metrics_insert AFTER INSERT ON notifications
    REFERENCING NEW TABLE AS added FOR EACH STATEMENT EXECUTE FUNCTION update_notification_metrics();
CREATE TRIGGER notification_metrics_update AFTER UPDATE ON notifications
    REFERENCING OLD TABLE AS removed NEW TABLE AS added FOR EACH STATEMENT EXECUTE FUNCTION update_notification_metrics();
CREATE TRIGGER notification_metrics_delete AFTER DELETE ON notifications
    REFERENCING OLD TABLE AS removed FOR EACH STATEMENT EXECUTE FUNCTION update_notification_metrics();
CREATE TRIGGER notification_metrics_truncate AFTER TRUNCATE ON notifications
    FOR EACH STATEMENT EXECUTE FUNCTION update_notification_metrics();

INSERT INTO notification_metric_totals
SELECT get_byte(uuid_send(id),0) % 32, status, notification_metric_bucket(status,updated_at-created_at),
       count(*), sum(CASE WHEN status='accepted' THEN extract(epoch FROM updated_at-created_at) ELSE 0 END)
FROM notifications GROUP BY 1,2,3;

CREATE INDEX status_events_retention ON status_events (changed_at,seq);
