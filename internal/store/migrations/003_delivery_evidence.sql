-- Existing notifications retain an empty reason until their next recorded result.
ALTER TABLE notifications
    ADD COLUMN correlation_id varchar(128) NOT NULL DEFAULT '',
    ADD COLUMN last_reason varchar(64) NOT NULL DEFAULT '',
    ADD COLUMN provider_http_status smallint CHECK (provider_http_status BETWEEN 100 AND 599);

ALTER TABLE status_events
    ADD COLUMN reason varchar(64),
    ADD COLUMN provider_http_status smallint CHECK (provider_http_status BETWEEN 100 AND 599);

CREATE OR REPLACE FUNCTION record_status_event() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.status IN ('retry_wait', 'accepted', 'failed', 'unknown') THEN
        INSERT INTO status_events(notification_id, attempt, status, reason, provider_http_status)
            VALUES (NEW.id, NEW.attempt, NEW.status, NULLIF(NEW.last_reason, ''), NEW.provider_http_status);
    ELSE
        INSERT INTO status_events(notification_id, attempt, status) VALUES (NEW.id, NEW.attempt, NEW.status);
    END IF;
    RETURN NULL;
END; $$;
