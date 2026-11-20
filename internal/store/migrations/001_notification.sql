CREATE TABLE batches (
    id uuid PRIMARY KEY,
    idempotency_key text NOT NULL UNIQUE,
    request_hash bytea NOT NULL,
    response jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE notifications (
    id uuid PRIMARY KEY,
    batch_id uuid NOT NULL REFERENCES batches(id),
    item_index integer NOT NULL,
    recipient text NOT NULL,
    channel text NOT NULL CHECK (channel IN ('sms','email','push')),
    content text NOT NULL,
    priority text NOT NULL CHECK (priority IN ('high','normal','low')),
    status text NOT NULL CHECK (status IN ('pending','scheduled','sending','retry_wait','accepted','failed','unknown','cancelled')),
    attempt integer NOT NULL CHECK (attempt > 0),
    message_id text NOT NULL DEFAULT '',
    scheduled_at timestamptz,
    next_attempt_at timestamptz,
    first_attempt_at timestamptz,
    traceparent text NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (batch_id, item_index)
);
CREATE INDEX notifications_list ON notifications (created_at DESC, id DESC);
CREATE INDEX notifications_status_list ON notifications (status, created_at DESC, id DESC);
CREATE INDEX notifications_channel_list ON notifications (channel, created_at DESC, id DESC);
CREATE INDEX notifications_due ON notifications (next_attempt_at) WHERE status IN ('scheduled','retry_wait');
CREATE INDEX notifications_sending ON notifications (updated_at) WHERE status = 'sending';

CREATE TABLE outbox (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregatetype text NOT NULL,
    aggregateid uuid NOT NULL,
    type text NOT NULL,
    payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE status_events (
    seq bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    notification_id uuid NOT NULL,
    attempt integer NOT NULL,
    status text NOT NULL,
    changed_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX status_events_notification ON status_events (notification_id, seq);
CREATE FUNCTION record_status_event() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO status_events(notification_id, attempt, status) VALUES (NEW.id, NEW.attempt, NEW.status);
    RETURN NULL;
END; $$;
CREATE TRIGGER notification_status_created AFTER INSERT ON notifications
    FOR EACH ROW EXECUTE FUNCTION record_status_event();
CREATE TRIGGER notification_status_changed AFTER UPDATE OF status ON notifications
    FOR EACH ROW WHEN (OLD.status IS DISTINCT FROM NEW.status) EXECUTE FUNCTION record_status_event();
