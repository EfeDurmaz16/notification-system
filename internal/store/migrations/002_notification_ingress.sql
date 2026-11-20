-- Rows and writers from the previous version use the existing priority topic.
ALTER TABLE notifications ADD COLUMN ingress text NOT NULL DEFAULT 'batch'
    CHECK (ingress IN ('batch', 'single'));
