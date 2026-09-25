-- =============================================================================
-- Fraud analytics model library (PostgreSQL)
--
-- Each query is preceded by "-- name: <id>" and is executed by the integration
-- test-suite against a seeded warehouse, so every statement here is known to run.
-- Look-back windows are relative to now() so the queries work on live data.
--
-- Ground truth: a transaction is "confirmed fraud" when a chargeback with a
-- fraud reason code (dw.dim_dispute_reason.is_fraud) references it.
-- =============================================================================


-- name: executive_kpi_snapshot
-- Business question: how is today trending versus the previous 24 hours?
-- Techniques: conditional aggregation (FILTER), CTEs, period-over-period deltas.
WITH labeled AS (
    SELECT f.event_ts,
           f.amount_usd,
           f.is_flagged,
           cb.chargeback_id IS NOT NULL AS is_confirmed_fraud,
           coalesce(cb.amount_usd, 0)   AS fraud_loss_usd
    FROM dw.fact_transactions f
    LEFT JOIN LATERAL (
        SELECT c.chargeback_id, c.amount_usd
        FROM dw.fact_chargebacks c
        JOIN dw.dim_dispute_reason r ON r.reason_code = c.reason_code AND r.is_fraud
        WHERE c.transaction_id = f.transaction_id
        LIMIT 1
    ) cb ON true
    WHERE f.event_ts >= now() - interval '48 hours'
), periods AS (
    SELECT CASE WHEN event_ts >= now() - interval '24 hours' THEN 'current' ELSE 'previous' END AS period,
           count(*)                                        AS txn_count,
           sum(amount_usd)                                 AS volume_usd,
           avg(amount_usd)                                 AS avg_ticket_usd,
           count(*) FILTER (WHERE is_flagged)              AS alerts,
           count(*) FILTER (WHERE is_confirmed_fraud)      AS confirmed_fraud,
           sum(fraud_loss_usd)                             AS fraud_loss_usd
    FROM labeled
    GROUP BY 1
)
SELECT c.txn_count,
       c.volume_usd,
       round(c.avg_ticket_usd, 2)                                               AS avg_ticket_usd,
       round(100.0 * c.alerts / nullif(c.txn_count, 0), 3)                      AS alert_rate_pct,
       round(10000.0 * c.confirmed_fraud / nullif(c.txn_count, 0), 2)           AS fraud_rate_bps,
       round(10000.0 * c.fraud_loss_usd / nullif(c.volume_usd, 0), 2)           AS fraud_loss_bps_of_volume,
       round(100.0 * (c.txn_count - p.txn_count) / nullif(p.txn_count, 0), 1)   AS txn_count_change_pct,
       round(100.0 * (c.volume_usd - p.volume_usd) / nullif(p.volume_usd, 0), 1) AS volume_change_pct
FROM periods c
LEFT JOIN periods p ON p.period = 'previous'
WHERE c.period = 'current';


-- name: trailing_30d_customer_fraud_rate
-- Business question: which cardholders carry the highest confirmed-fraud rate over
-- a trailing 30-day window (not a calendar month)?
-- Techniques: daily grain CTE, RANGE window frame over an interval, HAVING-style filter.
WITH daily AS (
    SELECT f.user_key,
           (f.event_ts AT TIME ZONE 'UTC')::date                                 AS day,
           count(*)                                                              AS txns,
           count(*) FILTER (WHERE EXISTS (
               SELECT 1 FROM dw.fact_chargebacks c
               JOIN dw.dim_dispute_reason r ON r.reason_code = c.reason_code AND r.is_fraud
               WHERE c.transaction_id = f.transaction_id))                       AS fraud_txns
    FROM dw.fact_transactions f
    WHERE f.event_ts >= now() - interval '60 days'
    GROUP BY 1, 2
), rolling AS (
    SELECT user_key,
           day,
           sum(txns)       OVER w AS txns_30d,
           sum(fraud_txns) OVER w AS fraud_txns_30d
    FROM daily
    WINDOW w AS (PARTITION BY user_key ORDER BY day RANGE BETWEEN INTERVAL '29 days' PRECEDING AND CURRENT ROW)
)
SELECT u.user_id,
       u.segment,
       u.card_tier,
       t.day                                                   AS as_of_day,
       t.txns_30d,
       t.fraud_txns_30d,
       round(100.0 * t.fraud_txns_30d / t.txns_30d, 2)         AS fraud_rate_30d_pct
FROM rolling t
JOIN dw.dim_users u ON u.user_key = t.user_key
WHERE t.day = (SELECT max(day) FROM daily)
  AND t.fraud_txns_30d > 0
ORDER BY fraud_rate_30d_pct DESC, t.fraud_txns_30d DESC
LIMIT 25;


-- name: riskiest_merchant_categories
-- Business question: which five merchant categories have the highest chargeback
-- probability - without small categories winning by luck?
-- Techniques: empirical-Bayes shrinkage toward the global rate, Wilson score lower
-- bound, RANK() window function.
WITH per_category AS (
    SELECT c.mcc,
           c.category_name,
           c.risk_tier,
           count(*)                                AS txns,
           count(cb.transaction_id)                AS fraud_chargebacks
    FROM dw.fact_transactions f
    JOIN dw.dim_merchants m          ON m.merchant_key = f.merchant_key
    JOIN dw.dim_merchant_category c  ON c.mcc = m.mcc
    LEFT JOIN (
        SELECT DISTINCT cb.transaction_id
        FROM dw.fact_chargebacks cb
        JOIN dw.dim_dispute_reason r ON r.reason_code = cb.reason_code AND r.is_fraud
    ) cb ON cb.transaction_id = f.transaction_id
    WHERE f.event_ts >= now() - interval '30 days'
    GROUP BY c.mcc, c.category_name, c.risk_tier
), prior AS (
    SELECT sum(fraud_chargebacks)::numeric / nullif(sum(txns), 0) AS global_rate,
           1000::numeric                                           AS prior_strength   -- pseudo-transactions
    FROM per_category
), scored AS (
    SELECT pc.*,
           pc.fraud_chargebacks::numeric / nullif(pc.txns, 0)                              AS raw_rate,
           (pc.fraud_chargebacks + p.prior_strength * p.global_rate) / (pc.txns + p.prior_strength) AS smoothed_rate,
           -- Wilson score interval lower bound at 95% confidence (z = 1.96)
           (   (pc.fraud_chargebacks::numeric / pc.txns) + 1.96 ^ 2 / (2 * pc.txns)
             - 1.96 * sqrt(((pc.fraud_chargebacks::numeric / pc.txns) * (1 - pc.fraud_chargebacks::numeric / pc.txns)
                           + 1.96 ^ 2 / (4 * pc.txns)) / pc.txns)
           ) / (1 + 1.96 ^ 2 / pc.txns)                                                   AS wilson_lower_bound
    FROM per_category pc
    CROSS JOIN prior p
    WHERE pc.txns > 0
)
SELECT rank() OVER (ORDER BY smoothed_rate DESC)          AS risk_rank,
       mcc,
       category_name,
       risk_tier,
       txns,
       fraud_chargebacks,
       round(100 * raw_rate, 4)                           AS raw_chargeback_pct,
       round(100 * smoothed_rate, 4)                      AS smoothed_chargeback_pct,
       round(100 * greatest(wilson_lower_bound, 0), 4)    AS wilson_lower_pct
FROM scored
ORDER BY risk_rank
LIMIT 5;


-- name: velocity_anomalies_consecutive_swipes
-- Business question: which consecutive swipes on the same card imply physically
-- impossible travel (faster than a commercial jet)?
-- Techniques: LAG()/LEAD() over a per-card ordering, haversine UDF, derived speed.
WITH swipes AS (
    SELECT f.user_key,
           f.transaction_id,
           f.event_ts,
           f.amount_usd,
           f.channel,
           l.city,
           f.txn_lat,
           f.txn_lon,
           lag(f.event_ts) OVER w   AS prev_ts,
           lag(f.txn_lat)  OVER w   AS prev_lat,
           lag(f.txn_lon)  OVER w   AS prev_lon,
           lag(l.city)     OVER w   AS prev_city,
           lead(l.city)    OVER w   AS next_city
    FROM dw.fact_transactions f
    JOIN dw.dim_location l ON l.location_key = f.location_key
    WHERE f.event_ts >= now() - interval '24 hours'
    WINDOW w AS (PARTITION BY f.user_key ORDER BY f.event_ts, f.transaction_id)
), legs AS (
    SELECT *,
           dw.haversine_km(prev_lat, prev_lon, txn_lat, txn_lon)       AS km,
           extract(epoch FROM event_ts - prev_ts)                      AS seconds
    FROM swipes
    WHERE prev_ts IS NOT NULL
)
SELECT u.user_id,
       prev_city || ' -> ' || city                                     AS route,
       next_city,
       prev_ts,
       event_ts,
       round(km::numeric, 1)                                           AS km,
       round(seconds::numeric / 60, 1)                                 AS minutes,
       round((km / (greatest(seconds, 60) / 3600.0))::numeric, 0)     AS implied_kmh,
       amount_usd,
       channel
FROM legs
JOIN dw.dim_users u ON u.user_key = legs.user_key
WHERE km > 300
  AND km / (greatest(seconds, 60) / 3600.0) > 900
ORDER BY implied_kmh DESC
LIMIT 50;


-- name: rule_effectiveness
-- Business question: how precise is each rule, how much fraud does it catch, and
-- how much better than random is it (lift)?
-- Techniques: UNNEST of an array column with LATERAL, confusion-matrix algebra, F1.
WITH labeled AS (
    SELECT f.transaction_id,
           f.reason_codes,
           EXISTS (SELECT 1 FROM dw.fact_chargebacks c
                   JOIN dw.dim_dispute_reason r ON r.reason_code = c.reason_code AND r.is_fraud
                   WHERE c.transaction_id = f.transaction_id) AS is_fraud
    FROM dw.fact_transactions f
    WHERE f.event_ts >= now() - interval '7 days'
      AND f.event_ts <  now() - interval '5 minutes'           -- label maturity
), base AS (
    SELECT count(*) AS n, count(*) FILTER (WHERE is_fraud) AS positives FROM labeled
), per_rule AS (
    SELECT rule,
           count(*)                            AS fired,
           count(*) FILTER (WHERE l.is_fraud)  AS tp
    FROM labeled l
    CROSS JOIN LATERAL unnest(l.reason_codes) AS r(rule)
    GROUP BY rule
)
SELECT pr.rule,
       pr.fired,
       pr.tp,
       pr.fired - pr.tp                                                        AS fp,
       round(pr.tp::numeric / pr.fired, 4)                                     AS precision,
       round(pr.tp::numeric / nullif(b.positives, 0), 4)                       AS recall,
       round(2.0 * pr.tp / nullif(pr.fired + b.positives, 0), 4)               AS f1,
       round((pr.tp::numeric / pr.fired) / nullif(b.positives::numeric / b.n, 0), 1) AS lift_over_base_rate
FROM per_rule pr
CROSS JOIN base b
ORDER BY precision DESC, pr.fired DESC;


-- name: alert_threshold_tuning_curve
-- Business question: what happens to precision, recall and analyst workload if we
-- move the alert threshold?
-- Techniques: generate_series CROSS JOIN for a parameter sweep, conditional counts.
WITH labeled AS (
    SELECT f.fraud_score,
           EXISTS (SELECT 1 FROM dw.fact_chargebacks c
                   JOIN dw.dim_dispute_reason r ON r.reason_code = c.reason_code AND r.is_fraud
                   WHERE c.transaction_id = f.transaction_id) AS is_fraud
    FROM dw.fact_transactions f
    WHERE f.event_ts >= now() - interval '7 days'
      AND f.event_ts <  now() - interval '5 minutes'
), thresholds AS (
    SELECT round(t::numeric, 2) AS threshold FROM generate_series(0.20, 0.95, 0.05) AS g(t)
)
SELECT t.threshold,
       count(*) FILTER (WHERE l.fraud_score >= t.threshold)                     AS alerts,
       count(*) FILTER (WHERE l.fraud_score >= t.threshold AND l.is_fraud)      AS true_positives,
       round(count(*) FILTER (WHERE l.fraud_score >= t.threshold AND l.is_fraud)::numeric
             / nullif(count(*) FILTER (WHERE l.fraud_score >= t.threshold), 0), 4) AS precision,
       round(count(*) FILTER (WHERE l.fraud_score >= t.threshold AND l.is_fraud)::numeric
             / nullif(count(*) FILTER (WHERE l.is_fraud), 0), 4)                AS recall,
       round(count(*) FILTER (WHERE l.fraud_score >= t.threshold AND NOT l.is_fraud)::numeric
             / nullif(count(*) FILTER (WHERE NOT l.is_fraud), 0), 6)            AS false_positive_rate
FROM thresholds t
CROSS JOIN labeled l
GROUP BY t.threshold
ORDER BY t.threshold;


-- name: fraud_incidents_time_to_detect
-- Business question: when a card is compromised, how many fraudulent swipes and
-- dollars slip through before the first alert fires?
-- Techniques: gaps-and-islands (LAG + running SUM) to group fraud into incidents,
-- ordered-set percentiles.
WITH fraud AS (
    SELECT f.user_key, f.transaction_id, f.event_ts, f.amount_usd, f.is_flagged
    FROM dw.fact_transactions f
    WHERE f.event_ts >= now() - interval '7 days'
      AND EXISTS (SELECT 1 FROM dw.fact_chargebacks c
                  JOIN dw.dim_dispute_reason r ON r.reason_code = c.reason_code AND r.is_fraud
                  WHERE c.transaction_id = f.transaction_id)
), marked AS (
    SELECT *,
           CASE WHEN event_ts - lag(event_ts) OVER (PARTITION BY user_key ORDER BY event_ts) <= interval '30 minutes'
                THEN 0 ELSE 1 END AS starts_incident
    FROM fraud
), incidents AS (
    SELECT *,
           sum(starts_incident) OVER (PARTITION BY user_key ORDER BY event_ts
                                      ROWS UNBOUNDED PRECEDING) AS incident_no
    FROM marked
), per_incident AS (
    SELECT user_key,
           incident_no,
           min(event_ts)                                                       AS started_at,
           count(*)                                                            AS fraud_txns,
           sum(amount_usd)                                                     AS fraud_usd,
           min(event_ts) FILTER (WHERE is_flagged)                             AS first_alert_at,
           count(*) FILTER (WHERE NOT is_flagged)                              AS undetected_txns
    FROM incidents
    GROUP BY user_key, incident_no
), measured AS (
    SELECT *,
           extract(epoch FROM first_alert_at - started_at)                     AS seconds_to_detect,
           (SELECT coalesce(sum(i.amount_usd), 0) FROM incidents i
             WHERE i.user_key = p.user_key AND i.incident_no = p.incident_no
               AND (p.first_alert_at IS NULL OR i.event_ts < p.first_alert_at)) AS usd_before_detection
    FROM per_incident p
)
SELECT count(*)                                                                AS incidents,
       count(*) FILTER (WHERE first_alert_at IS NOT NULL)                      AS detected_incidents,
       round(100.0 * count(*) FILTER (WHERE first_alert_at IS NOT NULL) / nullif(count(*), 0), 1) AS detection_rate_pct,
       round(avg(fraud_txns), 2)                                               AS avg_txns_per_incident,
       percentile_cont(0.5)  WITHIN GROUP (ORDER BY seconds_to_detect)         AS p50_seconds_to_detect,
       percentile_cont(0.9)  WITHIN GROUP (ORDER BY seconds_to_detect)         AS p90_seconds_to_detect,
       round(sum(usd_before_detection), 2)                                     AS usd_lost_before_detection,
       round(sum(fraud_usd), 2)                                                AS total_fraud_usd
FROM measured;


-- name: merchant_hourly_spike_zscores
-- Business question: which merchants are seeing volume far outside their own
-- normal pattern for this hour?
-- Techniques: hourly grain, trailing window AVG/STDDEV over prior rows, z-score.
WITH hourly AS (
    SELECT f.merchant_key,
           date_trunc('hour', f.event_ts)          AS hour_ts,
           count(*)                                AS txns,
           count(*) FILTER (WHERE f.is_flagged)    AS flagged
    FROM dw.fact_transactions f
    WHERE f.event_ts >= now() - interval '7 days'
    GROUP BY 1, 2
), stats AS (
    SELECT *,
           avg(txns)         OVER w AS baseline_avg,
           stddev_samp(txns) OVER w AS baseline_std,
           count(*)          OVER w AS baseline_hours
    FROM hourly
    WINDOW w AS (PARTITION BY merchant_key ORDER BY hour_ts ROWS BETWEEN 168 PRECEDING AND 1 PRECEDING)
)
SELECT m.merchant_name,
       c.category_name,
       s.hour_ts,
       s.txns,
       s.flagged,
       round(s.baseline_avg, 2)                                         AS baseline_avg,
       round(((s.txns - s.baseline_avg) / nullif(s.baseline_std, 0))::numeric, 2) AS zscore
FROM stats s
JOIN dw.dim_merchants m         ON m.merchant_key = s.merchant_key
JOIN dw.dim_merchant_category c ON c.mcc = m.mcc
WHERE s.baseline_hours >= 3
  AND s.hour_ts >= now() - interval '24 hours'
ORDER BY zscore DESC NULLS LAST
LIMIT 20;


-- name: sliding_window_merchant_risk_trend
-- Business question: how has each high-risk merchant's flagged share evolved across
-- the streaming 5-minute sliding windows, and is it accelerating?
-- Techniques: LAG() for window-over-window change, moving average, FIRST_VALUE.
SELECT m.merchant_name,
       w.window_start,
       w.txn_count,
       w.flagged_count,
       round(100.0 * w.flagged_count / nullif(w.txn_count, 0), 2)                         AS flagged_pct,
       w.flagged_count - lag(w.flagged_count) OVER mw                                     AS flagged_delta,
       round(avg(w.txn_count) OVER (mw ROWS BETWEEN 5 PRECEDING AND CURRENT ROW), 2)     AS txn_count_ma6,
       first_value(w.window_start) OVER (PARTITION BY w.merchant_key ORDER BY w.window_start) AS first_window
FROM dw.agg_merchant_window_risk w
JOIN dw.dim_merchants m         ON m.merchant_key = w.merchant_key
JOIN dw.dim_merchant_category c ON c.mcc = m.mcc AND c.risk_tier = 'HIGH'
WHERE w.window_start >= now() - interval '2 hours'
WINDOW mw AS (PARTITION BY w.merchant_key ORDER BY w.window_start)
ORDER BY m.merchant_name, w.window_start;


-- name: customer_spend_deciles
-- Business question: is fraud concentrated among heavy or light spenders?
-- Techniques: NTILE(10) segmentation, per-decile rates, cumulative share (running SUM).
WITH spend AS (
    SELECT f.user_key,
           sum(f.amount_usd)                    AS spend_usd,
           count(*)                             AS txns,
           count(*) FILTER (WHERE f.is_flagged) AS alerts
    FROM dw.fact_transactions f
    WHERE f.event_ts >= now() - interval '30 days'
    GROUP BY f.user_key
), deciles AS (
    SELECT *, ntile(10) OVER (ORDER BY spend_usd) AS spend_decile FROM spend
), agg AS (
    SELECT spend_decile,
           count(*)          AS cardholders,
           sum(spend_usd)    AS spend_usd,
           sum(txns)         AS txns,
           sum(alerts)       AS alerts
    FROM deciles
    GROUP BY spend_decile
)
SELECT spend_decile,
       cardholders,
       round(spend_usd, 2)                                                     AS spend_usd,
       round(100.0 * spend_usd / sum(spend_usd) OVER (), 2)                    AS share_of_spend_pct,
       round(100.0 * sum(spend_usd) OVER (ORDER BY spend_decile DESC) / sum(spend_usd) OVER (), 2) AS cumulative_share_from_top_pct,
       round(100.0 * alerts / nullif(txns, 0), 3)                              AS alert_rate_pct
FROM agg
ORDER BY spend_decile;


-- name: geography_rollup
-- Business question: where does volume and risk come from, with country subtotals
-- and a grand total in one result?
-- Techniques: GROUP BY ROLLUP with GROUPING() to label subtotal rows.
SELECT CASE WHEN grouping(l.country_code) = 1 THEN 'ALL' ELSE l.country_code END           AS country,
       CASE WHEN grouping(l.city) = 1 THEN
                CASE WHEN grouping(l.country_code) = 1 THEN 'Grand total' ELSE 'Country total' END
            ELSE l.city END                                                                AS city,
       count(*)                                                                            AS txns,
       round(sum(f.amount_usd), 2)                                                         AS volume_usd,
       round(100.0 * count(*) FILTER (WHERE f.is_flagged) / count(*), 3)                   AS alert_rate_pct
FROM dw.fact_transactions f
JOIN dw.dim_location l ON l.location_key = f.location_key
WHERE f.event_ts >= now() - interval '24 hours'
GROUP BY ROLLUP (l.country_code, l.city)
-- Grand total first, then each country's subtotal followed by its cities.
ORDER BY grouping(l.country_code) DESC, country, grouping(l.city) DESC, txns DESC;


-- name: chargeback_latency_by_reason
-- Business question: how long after the purchase do disputes arrive? This sets the
-- label-maturity window used when measuring rule precision.
-- Techniques: ordered-set aggregates (percentile_cont) per group.
SELECT r.reason_code,
       r.description,
       r.is_fraud,
       count(*)                                                                             AS chargebacks,
       round(percentile_cont(0.5)  WITHIN GROUP (ORDER BY extract(epoch FROM c.reported_ts - f.event_ts))::numeric, 1) AS p50_seconds,
       round(percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM c.reported_ts - f.event_ts))::numeric, 1) AS p95_seconds,
       round(sum(c.amount_usd), 2)                                                          AS disputed_usd
FROM dw.fact_chargebacks c
JOIN dw.dim_dispute_reason r    ON r.reason_code = c.reason_code
JOIN dw.fact_transactions f     ON f.transaction_id = c.transaction_id
GROUP BY r.reason_code, r.description, r.is_fraud
ORDER BY chargebacks DESC;


-- name: weekday_hour_risk_heatmap
-- Business question: when (day of week x hour) is fraud pressure highest?
-- Techniques: conformed date dimension join, two-dimensional pivot-ready grain.
SELECT d.day_name,
       d.day_of_week,
       extract(hour FROM f.event_ts AT TIME ZONE 'UTC')::int                    AS hour_utc,
       count(*)                                                                AS txns,
       count(*) FILTER (WHERE f.is_flagged)                                    AS alerts,
       round(100.0 * count(*) FILTER (WHERE f.is_flagged) / count(*), 3)       AS alert_rate_pct
FROM dw.fact_transactions f
JOIN dw.dim_date d ON d.date_key = f.date_key
WHERE f.event_ts >= now() - interval '28 days'
GROUP BY d.day_name, d.day_of_week, hour_utc
ORDER BY d.day_of_week, hour_utc;


-- name: scd2_card_tier_history
-- Business question: which cardholders changed card tier, and what did the fraud
-- profile look like under each version? (Facts join the version valid at swipe time.)
-- Techniques: SCD Type 2 point-in-time join, LEAD() to show the next version.
SELECT u.user_id,
       u.card_tier,
       lead(u.card_tier) OVER (PARTITION BY u.user_id ORDER BY u.valid_from) AS next_tier,
       u.valid_from,
       u.valid_to,
       u.is_current,
       count(f.transaction_id)                                               AS txns_under_version,
       count(f.transaction_id) FILTER (WHERE f.is_flagged)                   AS alerts_under_version
FROM dw.dim_users u
LEFT JOIN dw.fact_transactions f ON f.user_key = u.user_key
WHERE u.user_id IN (SELECT user_id FROM dw.dim_users GROUP BY user_id HAVING count(*) > 1)
GROUP BY u.user_key, u.user_id, u.card_tier, u.valid_from, u.valid_to, u.is_current
ORDER BY u.user_id, u.valid_from
LIMIT 100;


-- name: pipeline_latency_slo
-- Business question: is the streaming platform meeting its latency SLO
-- (micro-batch < 2 s) per query, per 5-minute bucket?
-- Techniques: date_bin bucketing, percentile_cont, SLO attainment ratio.
SELECT query_name,
       date_bin('5 minutes', progress_ts, timestamptz '2000-01-01')          AS bucket,
       count(*)                                                             AS batches,
       sum(num_input_rows)                                                  AS rows_processed,
       round((sum(num_input_rows) / 300.0)::numeric, 1)                     AS avg_rows_per_second,
       percentile_cont(0.50) WITHIN GROUP (ORDER BY batch_duration_ms)      AS p50_ms,
       percentile_cont(0.95) WITHIN GROUP (ORDER BY batch_duration_ms)      AS p95_ms,
       percentile_cont(0.99) WITHIN GROUP (ORDER BY batch_duration_ms)      AS p99_ms,
       round(100.0 * count(*) FILTER (WHERE batch_duration_ms < 2000) / count(*), 2) AS slo_attainment_pct
FROM ops.streaming_query_progress
WHERE progress_ts >= now() - interval '24 hours'
  AND num_input_rows > 0
GROUP BY query_name, bucket
ORDER BY query_name, bucket;
