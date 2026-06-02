-- GEX Webhook Pipeline - Audit Queries
-- ----------------------------------------------------------------------------
-- Each query answers one question from the challenge (section 2.q).
-- All queries must run in < 1 second on the provided dataset (200 webhooks).
-- EXPLAIN output is captured separately and documented in docs/explicativo.md.
-- ----------------------------------------------------------------------------


-- ============================================================================
-- Query 1: Average SMS lag (DB persist -> channel delivery) by gateway,
--          in the last 24 hours.
-- Question: "How fast does the pipeline go from gateway receipt to the
--           customer receiving an SMS, broken down by gateway?"
-- ============================================================================
SELECT
    o.gateway,
    ROUND(AVG(ds.lag_db_to_channel_seconds), 3) AS avg_lag_seconds,
    COUNT(*) AS delivered_count
FROM distribution_status ds
JOIN orders o ON o.id = ds.order_id
WHERE ds.channel = 'SMS'
  AND ds.status  = 'delivered'
  AND ds.delivered_at >= NOW() - INTERVAL 24 HOUR
GROUP BY o.gateway
ORDER BY avg_lag_seconds DESC;

-- EXPLAIN Query 1
-- (run separately, then recorded in docs/explicativo.md)
EXPLAIN
SELECT
    o.gateway,
    ROUND(AVG(ds.lag_db_to_channel_seconds), 3) AS avg_lag_seconds,
    COUNT(*) AS delivered_count
FROM distribution_status ds
JOIN orders o ON o.id = ds.order_id
WHERE ds.channel = 'SMS'
  AND ds.status  = 'delivered'
  AND ds.delivered_at >= NOW() - INTERVAL 24 HOUR
GROUP BY o.gateway
ORDER BY avg_lag_seconds DESC;


-- ============================================================================
-- Query 2: Leads pending in any channel for more than 5 minutes.
-- Question: "Which orders are stuck waiting for a channel that never picked
--           them up? This is the alert query for the on-call dashboard."
-- ============================================================================
SELECT
    o.id                                AS order_id,
    o.transaction_id,
    ds.channel,
    ds.status,
    TIMESTAMPDIFF(SECOND, ds.created_at, NOW()) AS pending_age_seconds,
    ds.created_at                       AS pending_since
FROM distribution_status ds
JOIN orders o ON o.id = ds.order_id
WHERE ds.status    = 'pending'
  AND ds.created_at < NOW() - INTERVAL 5 MINUTE
ORDER BY pending_age_seconds DESC
LIMIT 100;

-- EXPLAIN Query 2
EXPLAIN
SELECT
    o.id                                AS order_id,
    o.transaction_id,
    ds.channel,
    ds.status,
    TIMESTAMPDIFF(SECOND, ds.created_at, NOW()) AS pending_age_seconds,
    ds.created_at                       AS pending_since
FROM distribution_status ds
JOIN orders o ON o.id = ds.order_id
WHERE ds.status    = 'pending'
  AND ds.created_at < NOW() - INTERVAL 5 MINUTE
ORDER BY pending_age_seconds DESC
LIMIT 100;


-- ============================================================================
-- Query 3: SMS success rate by product, by hour, in the last 6 hours.
-- Question: "Is the SMS provider failing more for one product than another?
--           What is the hourly pattern?"
-- ============================================================================
SELECT
    o.product_id,
    o.product_name,
    DATE_FORMAT(ds.created_at, '%Y-%m-%d %H:00:00') AS hour_bucket,
    COUNT(*) AS total,
    SUM(CASE WHEN ds.status = 'delivered' THEN 1 ELSE 0 END) AS delivered,
    SUM(CASE WHEN ds.status = 'failed'    THEN 1 ELSE 0 END) AS failed,
    SUM(CASE WHEN ds.status = 'pending'   THEN 1 ELSE 0 END) AS pending,
    ROUND(
        100.0 * SUM(CASE WHEN ds.status = 'delivered' THEN 1 ELSE 0 END) / COUNT(*),
        2
    ) AS success_rate_pct
FROM distribution_status ds
JOIN orders o ON o.id = ds.order_id
WHERE ds.channel    = 'SMS'
  AND ds.created_at >= NOW() - INTERVAL 6 HOUR
GROUP BY o.product_id, o.product_name, hour_bucket
ORDER BY hour_bucket, o.product_id;

-- EXPLAIN Query 3
EXPLAIN
SELECT
    o.product_id,
    o.product_name,
    DATE_FORMAT(ds.created_at, '%Y-%m-%d %H:00:00') AS hour_bucket,
    COUNT(*) AS total,
    SUM(CASE WHEN ds.status = 'delivered' THEN 1 ELSE 0 END) AS delivered,
    SUM(CASE WHEN ds.status = 'failed'    THEN 1 ELSE 0 END) AS failed,
    SUM(CASE WHEN ds.status = 'pending'   THEN 1 ELSE 0 END) AS pending,
    ROUND(
        100.0 * SUM(CASE WHEN ds.status = 'delivered' THEN 1 ELSE 0 END) / COUNT(*),
        2
    ) AS success_rate_pct
FROM distribution_status ds
JOIN orders o ON o.id = ds.order_id
WHERE ds.channel    = 'SMS'
  AND ds.created_at >= NOW() - INTERVAL 6 HOUR
GROUP BY o.product_id, o.product_name, hour_bucket
ORDER BY hour_bucket, o.product_id;


-- ============================================================================
-- Query 4: DLQ counts by reason (origin) in the last 24 hours.
-- Question: "Are we losing more leads to decrypt failures or consumer
--           failures? Where is the pipeline leaking?"
-- ============================================================================
SELECT
    origin,
    COUNT(*) AS total,
    MIN(created_at) AS earliest,
    MAX(created_at) AS latest
FROM lead_dead_letter
WHERE created_at >= NOW() - INTERVAL 24 HOUR
GROUP BY origin
ORDER BY total DESC;

-- EXPLAIN Query 4
EXPLAIN
SELECT
    origin,
    COUNT(*) AS total,
    MIN(created_at) AS earliest,
    MAX(created_at) AS latest
FROM lead_dead_letter
WHERE created_at >= NOW() - INTERVAL 24 HOUR
GROUP BY origin
ORDER BY total DESC;


-- ============================================================================
-- Query 5: Reconciliation — approved events vs delivered SMS, by day,
--          in the last 7 days. Shows the absolute and percentage gap.
-- Question: "For every day in the last week, how many approved events
--           became a delivered SMS? Where is the loss?"
-- ============================================================================
SELECT
    DATE(le.db_timestamp)                       AS day,
    COUNT(DISTINCT le.order_id)                 AS approved_count,
    COUNT(DISTINCT ds.order_id)                 AS delivered_sms_count,
    COUNT(DISTINCT le.order_id)
        - COUNT(DISTINCT ds.order_id)           AS gap_abs,
    ROUND(
        100.0 * (
            COUNT(DISTINCT le.order_id) - COUNT(DISTINCT ds.order_id)
        ) / NULLIF(COUNT(DISTINCT le.order_id), 0),
        2
    )                                            AS gap_pct
FROM lead_events le
LEFT JOIN distribution_status ds
       ON ds.order_id = le.order_id
      AND ds.channel  = 'SMS'
      AND ds.status   = 'delivered'
WHERE le.event        = 'order.approved'
  AND le.db_timestamp >= NOW() - INTERVAL 7 DAY
GROUP BY DATE(le.db_timestamp)
ORDER BY day;

-- EXPLAIN Query 5
EXPLAIN
SELECT
    DATE(le.db_timestamp)                       AS day,
    COUNT(DISTINCT le.order_id)                 AS approved_count,
    COUNT(DISTINCT ds.order_id)                 AS delivered_sms_count,
    COUNT(DISTINCT le.order_id)
        - COUNT(DISTINCT ds.order_id)           AS gap_abs,
    ROUND(
        100.0 * (
            COUNT(DISTINCT le.order_id) - COUNT(DISTINCT ds.order_id)
        ) / NULLIF(COUNT(DISTINCT le.order_id), 0),
        2
    )                                            AS gap_pct
FROM lead_events le
LEFT JOIN distribution_status ds
       ON ds.order_id = le.order_id
      AND ds.channel  = 'SMS'
      AND ds.status   = 'delivered'
WHERE le.event        = 'order.approved'
  AND le.db_timestamp >= NOW() - INTERVAL 7 DAY
GROUP BY DATE(le.db_timestamp)
ORDER BY day;
