-- GEX Webhook Pipeline - Index Justifications
-- ----------------------------------------------------------------------------
-- All primary indexes are created inline in 001_create_tables.sql. This file
-- (1) documents the rationale for each, and
-- (2) applies additional covering indexes that were not co-located with the
--     table definition. Each justification follows the format:
--
--     INDEX NAME
--     Table: <table>
--     Columns: <columns> (column order matters for composite indexes)
--     Purpose: <one-line statement>
--     Query served: <a representative query that uses this index>
--     Why this column order: <equality cols first, range cols last>
--     Without it: <cost of the table scan / filesort>
-- ----------------------------------------------------------------------------

-- ============================================================================
-- raw_payloads
-- ============================================================================

-- idx_raw_gateway_time
--   Table: raw_payloads
--   Columns: (gateway, received_at)
--   Purpose: Bounded audit queries "show me everything from gateway X in the
--            last 24 hours".
--   Query:   SELECT * FROM raw_payloads
--            WHERE gateway = 'grummer'
--              AND received_at >= NOW() - INTERVAL 24 HOUR;
--   Column order: gateway is equality (low cardinality, but typically only 2
--                 values), received_at is range. Equality first lets MySQL
--                 seek into the index then range-scan.
--   Without it: full table scan across all webhooks ever received.

-- idx_raw_status
--   Table: raw_payloads
--   Columns: (processing_status)
--   Purpose: Count payloads by outcome ("how many duplicates today?").
--   Query:   SELECT processing_status, COUNT(*) FROM raw_payloads
--            WHERE processing_status IN ('accepted', 'duplicate')
--            GROUP BY processing_status;
--   Without it: full table scan to group by status.

-- idx_raw_correlation
--   Table: raw_payloads
--   Columns: (correlation_id)
--   Purpose: Trace a single request from HTTP receipt to its DB record by
--            correlation_id, and prove the chain end-to-end in the Loom demo.
--   Query:   SELECT * FROM raw_payloads WHERE correlation_id = ?;
--   Without it: full table scan for a single-row lookup.

-- idx_raw_received_at
--   Table: raw_payloads
--   Columns: (received_at)
--   Purpose: Global time-window scans when gateway is not filtered.
--   Query:   SELECT COUNT(*) FROM raw_payloads
--            WHERE received_at >= NOW() - INTERVAL 1 HOUR;
--   Without it: full table scan.


-- ============================================================================
-- processed_events
-- ============================================================================

-- uk_processed_events_natural
--   Table: processed_events
--   Columns: UNIQUE (gateway, transaction_id, event)
--   Purpose: THE receiver-layer idempotency constraint. INSERT ... ON
--            DUPLICATE KEY UPDATE short-circuits on this index.
--   Query:   INSERT INTO processed_events (...) VALUES (...)
--            AS new ON DUPLICATE KEY UPDATE correlation_id = new.correlation_id;
--   Column order: gateway equality (2 values), transaction_id equality
--                 (string up to 128 chars), event equality (short string).
--                 All equality, so the leading column is the one with the
--                 lowest cardinality (gateway) only because of convention —
--                 the index works regardless of order in an all-equality
--                 lookup.
--   Without it: the HTTP handler cannot enforce idempotency at the DB level
--               and race conditions on duplicate webhooks would slip through.

-- idx_processed_events_correlation
--   Table: processed_events
--   Columns: (correlation_id)
--   Purpose: Same tracing rationale as idx_raw_correlation.


-- ============================================================================
-- leads
-- ============================================================================

-- uk_leads_email
--   Table: leads
--   Columns: UNIQUE (email)
--   Purpose: Customer dedup — leads buying from multiple gateways with the
--            same email collapse to one row.
--   Query:   SELECT id FROM leads WHERE email = ?;
--   Without it: customer duplicates on case/space variants would pile up.


-- ============================================================================
-- orders
-- ============================================================================

-- uk_orders_gateway_txn
--   Table: orders
--   Columns: UNIQUE (gateway, transaction_id)
--   Purpose: Order dedup by natural key. The same physical order is
--            re-received on retry or webhook replay, and we must not create
--            two orders.
--   Query:   SELECT id FROM orders
--            WHERE gateway = ? AND transaction_id = ?;
--   Without it: one order becomes two after a webhook retry.

-- idx_orders_lead
--   Table: orders
--   Columns: (lead_id)
--   Purpose: Join orders -> leads to fetch customer info per order.
--   Query:   SELECT o.*, l.email FROM orders o JOIN leads l ON o.lead_id = l.id;

-- idx_orders_product
--   Table: orders
--   Columns: (product_id)
--   Purpose: Aggregation queries by product (success rate, sales volume).
--   Query:   SELECT product_id, COUNT(*), SUM(amount_usd) FROM orders
--            WHERE created_at >= NOW() - INTERVAL 7 DAY
--            GROUP BY product_id;

-- idx_orders_correlation
--   Table: orders
--   Columns: (correlation_id)
--   Purpose: Trace an order back to the HTTP request that produced it.

-- idx_orders_event
--   Table: orders
--   Columns: (event)
--   Purpose: Filter "all refunds", "all chargebacks", etc.

-- idx_orders_gateway_created
--   Table: orders
--   Columns: (gateway, created_at)
--   Purpose: Lag analysis by gateway ("average time from order to DB
--            persistence for grummer vs lous").
--   Query:   SELECT gateway, AVG(TIMESTAMPDIFF(MICROSECOND, transaction_time,
--            created_at) / 1e6) FROM orders
--            WHERE created_at >= NOW() - INTERVAL 24 HOUR
--            GROUP BY gateway;


-- ============================================================================
-- lead_events
-- ============================================================================

-- uk_lead_events_order_event
--   Table: lead_events
--   Columns: UNIQUE (order_id, event)
--   Purpose: THE worker's idempotency key. Even if the consumer is
--            horizontally scaled and a message is delivered twice, the DB
--            guarantees one (order, event) row.
--   Without it: a refunded event would create two refund records and the
--               audit query "count approved events" would double-count.

-- idx_lead_events_correlation
--   Table: lead_events
--   Columns: (correlation_id)
--   Purpose: Trace a webhook through to its persisted event.

-- idx_lead_events_event
--   Table: lead_events
--   Columns: (event)
--   Purpose: Filter by event type ("all order.approved events").

-- idx_lead_events_event_time
--   Table: lead_events
--   Columns: (event, gateway_timestamp)
--   Purpose: Time-bounded event audit (audit query #1: lag by gateway in 24h).
--   Query:   SELECT gateway, AVG(lag_seconds) ... WHERE event = 'order.approved'
--              AND gateway_timestamp >= NOW() - INTERVAL 24 HOUR;
--   Column order: event equality first, gateway_timestamp range second.


-- ============================================================================
-- distribution_status
-- ============================================================================

-- uk_dist_order_channel
--   Table: distribution_status
--   Columns: UNIQUE (order_id, channel)
--   Purpose: Exactly one status row per (order, channel) so retry/republish
--            cannot insert duplicates.

-- idx_dist_order
--   Table: distribution_status
--   Columns: (order_id)
--   Purpose: Lookup all channels for an order.

-- idx_dist_channel_status_time
--   Table: distribution_status
--   Columns: (channel, status, created_at)
--   Purpose: Audit query #2 (pending > 5 min) and #3 (success rate by
--            product/hour) both filter by channel + status + time window.
--   Query:   SELECT order_id, channel,
--            TIMESTAMPDIFF(SECOND, created_at, NOW()) AS pending_age
--            FROM distribution_status
--            WHERE status = 'pending' AND created_at < NOW() - INTERVAL 5 MINUTE;
--   Column order: channel equality, status equality, created_at range.

-- idx_dist_delivered_at
--   Table: distribution_status
--   Columns: (channel, delivered_at)
--   Purpose: Delivered-time scans for lag-from-delivery analytics
--            ("average time from DB persist to SMS confirmation in 24h").
--   Query:   SELECT channel, AVG(lag_db_to_channel_seconds)
--            FROM distribution_status
--            WHERE status = 'delivered'
--              AND delivered_at >= NOW() - INTERVAL 24 HOUR
--            GROUP BY channel;

-- idx_dist_pending_since
--   Table: distribution_status
--   Columns: (status, created_at)
--   Purpose: Specifically targets the "pending older than 5 min" alert query.
--            This is a leading-prefix subset of idx_dist_channel_status_time;
--            the dedicated index avoids scanning channel partitions when the
--            alert system queries all channels at once.
--   Query:   SELECT order_id, channel FROM distribution_status
--            WHERE status = 'pending' AND created_at < NOW() - INTERVAL 5 MINUTE;


-- ============================================================================
-- lead_dead_letter
-- ============================================================================

-- idx_dlq_origin_time
--   Table: lead_dead_letter
--   Columns: (origin, created_at)
--   Purpose: Audit query #4 ("DLQ by reason in 24h").
--   Query:   SELECT origin, COUNT(*) FROM lead_dead_letter
--            WHERE created_at >= NOW() - INTERVAL 24 HOUR
--            GROUP BY origin;
--   Column order: origin equality, created_at range.

-- idx_dlq_correlation
--   Table: lead_dead_letter
--   Columns: (correlation_id)
--   Purpose: Trace a failed request to its DLQ record.

-- idx_dlq_created_at
--   Table: lead_dead_letter
--   Columns: (created_at)
--   Purpose: Global DLQ window scans when origin is not filtered.
