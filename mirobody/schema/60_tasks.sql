-- A claimed task stays in the queue until its consumer acknowledges it.
-- Lease expiry makes an interrupted batch eligible for another worker.
CREATE TABLE IF NOT EXISTS th_task_queue (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    queue_name TEXT NOT NULL,
    payload TEXT NOT NULL,
    available_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    lease_token UUID,
    attempts INTEGER NOT NULL DEFAULT 0,
    failed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_th_task_queue_ready
ON th_task_queue (queue_name, available_at, id DESC)
WHERE failed_at IS NULL;
