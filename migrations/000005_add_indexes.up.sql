CREATE INDEX IF NOT EXISTS idx_delivery_jobs_pending ON delivery_jobs(next_attempt ASC) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS idx_requests_endpoint_received ON requests(endpoint_id, received_at DESC);
