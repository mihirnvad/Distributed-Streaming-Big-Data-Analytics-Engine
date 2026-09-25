"""SQL behind every dashboard panel.

Kept separate from the Streamlit layout so the queries can be executed and
asserted against a real Postgres in the integration tests. Every query takes the
same named parameters:

    window  interval text for the look-back, e.g. '1 hour'
    bucket  interval text for time bucketing, e.g. '5 minutes'
"""

from __future__ import annotations

from dataclasses import dataclass

# Chargebacks arrive with a delay; transactions younger than this are excluded from
# label-based metrics so precision is not understated by not-yet-reported fraud.
LABEL_MATURITY = "5 minutes"


@dataclass(frozen=True)
class TimeWindow:
    label: str
    window: str
    bucket: str


TIME_WINDOWS = (
    TimeWindow("Last 15 minutes", "15 minutes", "1 minute"),
    TimeWindow("Last hour", "1 hour", "5 minutes"),
    TimeWindow("Last 6 hours", "6 hours", "30 minutes"),
    TimeWindow("Last 24 hours", "24 hours", "1 hour"),
)

KPI_SUMMARY = """
WITH txn AS (
    SELECT count(*)                                AS txn_count,
           coalesce(sum(amount_usd), 0)            AS volume_usd,
           count(*) FILTER (WHERE is_flagged)      AS alert_count,
           coalesce(sum(amount_usd) FILTER (WHERE is_flagged), 0) AS flagged_usd
    FROM dw.fact_transactions
    WHERE event_ts >= now() - %(window)s::interval
), cb AS (
    SELECT count(*) FILTER (WHERE r.is_fraud)      AS fraud_chargebacks,
           coalesce(sum(c.amount_usd) FILTER (WHERE r.is_fraud), 0) AS fraud_chargeback_usd
    FROM dw.fact_chargebacks c
    JOIN dw.dim_dispute_reason r ON r.reason_code = c.reason_code
    WHERE c.reported_ts >= now() - %(window)s::interval
)
SELECT txn.*, cb.*,
       CASE WHEN txn.txn_count > 0 THEN txn.alert_count::numeric / txn.txn_count END AS alert_rate
FROM txn CROSS JOIN cb
"""

THROUGHPUT = """
SELECT coalesce(sum(num_input_rows), 0) / 60.0 AS events_per_second
FROM ops.streaming_query_progress
WHERE query_name = 'bronze_transactions'
  AND progress_ts >= now() - interval '60 seconds'
"""

# End-to-end latency: Kafka append time -> row committed in Postgres.
END_TO_END_LATENCY = """
SELECT count(*) AS sample_rows,
       percentile_cont(0.50) WITHIN GROUP (ORDER BY extract(epoch FROM loaded_at - kafka_ts)) AS p50_s,
       percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM loaded_at - kafka_ts)) AS p95_s
FROM dw.fact_transactions
WHERE event_ts >= now() - interval '30 minutes'   -- partition pruning
  AND loaded_at >= now() - interval '5 minutes'
"""

MICRO_BATCH_LATENCY = """
SELECT percentile_cont(0.50) WITHIN GROUP (ORDER BY batch_duration_ms) / 1000.0 AS p50_s,
       percentile_cont(0.95) WITHIN GROUP (ORDER BY batch_duration_ms) / 1000.0 AS p95_s,
       count(*) AS batches
FROM ops.streaming_query_progress
WHERE query_name = 'gold_transactions'
  AND num_input_rows > 0
  AND progress_ts >= now() - %(window)s::interval
"""

TIMESERIES = """
SELECT date_bin(%(bucket)s::interval, event_ts, timestamptz '2000-01-01') AS bucket,
       count(*)                                        AS txn_count,
       count(*) FILTER (WHERE is_flagged)              AS alert_count,
       round(100.0 * count(*) FILTER (WHERE is_flagged) / count(*), 3) AS alert_rate_pct
FROM dw.fact_transactions
WHERE event_ts >= now() - %(window)s::interval
GROUP BY 1
ORDER BY 1
"""

ALERT_FEED = """
SELECT a.created_at,
       a.event_ts,
       a.risk_level,
       a.fraud_score,
       a.amount_usd,
       array_to_string(a.reason_codes, ', ') AS reasons,
       u.user_id,
       m.merchant_name,
       c.category_name,
       a.status
FROM dw.fact_fraud_alerts a
JOIN dw.dim_users u              ON u.user_key = a.user_key
JOIN dw.dim_merchants m          ON m.merchant_key = a.merchant_key
JOIN dw.dim_merchant_category c  ON c.mcc = m.mcc
WHERE a.created_at >= now() - %(window)s::interval
ORDER BY a.created_at DESC, a.fraud_score DESC
LIMIT 50
"""

CATEGORY_RISK_HEATMAP = """
SELECT date_bin(%(bucket)s::interval, f.event_ts, timestamptz '2000-01-01') AS bucket,
       c.category_name,
       count(*)                           AS txn_count,
       count(*) FILTER (WHERE f.is_flagged) AS alert_count,
       round(100.0 * count(*) FILTER (WHERE f.is_flagged) / count(*), 3) AS alert_rate_pct
FROM dw.fact_transactions f
JOIN dw.dim_merchants m          ON m.merchant_key = f.merchant_key
JOIN dw.dim_merchant_category c  ON c.mcc = m.mcc
WHERE f.event_ts >= now() - %(window)s::interval
GROUP BY 1, 2
"""

RULE_PERFORMANCE = f"""
WITH labeled AS (
    SELECT f.transaction_id,
           f.reason_codes,
           f.is_flagged,
           EXISTS (
               SELECT 1 FROM dw.fact_chargebacks cb
               JOIN dw.dim_dispute_reason r ON r.reason_code = cb.reason_code AND r.is_fraud
               WHERE cb.transaction_id = f.transaction_id
           ) AS is_fraud
    FROM dw.fact_transactions f
    WHERE f.event_ts >= now() - %(window)s::interval
      AND f.event_ts <  now() - interval '{LABEL_MATURITY}'
), totals AS (
    SELECT count(*) FILTER (WHERE is_fraud) AS fraud_total FROM labeled
)
SELECT rule,
       count(*)                                  AS hits,
       count(*) FILTER (WHERE l.is_fraud)        AS true_positives,
       round(count(*) FILTER (WHERE l.is_fraud)::numeric / count(*), 4) AS precision,
       round(count(*) FILTER (WHERE l.is_fraud)::numeric / nullif(t.fraud_total, 0), 4) AS recall
FROM labeled l
CROSS JOIN LATERAL unnest(l.reason_codes) AS r(rule)
CROSS JOIN totals t
GROUP BY rule, t.fraud_total
ORDER BY hits DESC
"""

MODEL_CONFUSION = f"""
WITH labeled AS (
    SELECT f.is_flagged,
           EXISTS (
               SELECT 1 FROM dw.fact_chargebacks cb
               JOIN dw.dim_dispute_reason r ON r.reason_code = cb.reason_code AND r.is_fraud
               WHERE cb.transaction_id = f.transaction_id
           ) AS is_fraud
    FROM dw.fact_transactions f
    WHERE f.event_ts >= now() - %(window)s::interval
      AND f.event_ts <  now() - interval '{LABEL_MATURITY}'
)
SELECT count(*) FILTER (WHERE is_flagged AND is_fraud)          AS tp,
       count(*) FILTER (WHERE is_flagged AND NOT is_fraud)      AS fp,
       count(*) FILTER (WHERE NOT is_flagged AND is_fraud)      AS fn,
       count(*) FILTER (WHERE NOT is_flagged AND NOT is_fraud)  AS tn
FROM labeled
"""

# Merchants whose latest sliding window deviates most from their own trailing baseline.
MERCHANT_SPIKES = """
WITH recent AS (
    SELECT w.*,
           avg(w.txn_count) OVER (
               PARTITION BY w.merchant_key ORDER BY w.window_start
               ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
           ) AS baseline_txn_count,
           row_number() OVER (PARTITION BY w.merchant_key ORDER BY w.window_start DESC) AS rn
    FROM dw.agg_merchant_window_risk w
    WHERE w.window_start >= now() - interval '45 minutes'
      AND w.window_end <= now() + interval '1 minute'
)
SELECT m.merchant_name,
       c.category_name,
       r.window_start,
       r.txn_count,
       round(r.baseline_txn_count, 2) AS baseline_txn_count,
       round(r.txn_count / nullif(r.baseline_txn_count, 0), 2) AS spike_ratio,
       r.flagged_count,
       r.avg_fraud_score,
       r.total_amount_usd
FROM recent r
JOIN dw.dim_merchants m         ON m.merchant_key = r.merchant_key
JOIN dw.dim_merchant_category c ON c.mcc = m.mcc
WHERE r.rn = 1
ORDER BY r.flagged_count DESC, spike_ratio DESC NULLS LAST
LIMIT 15
"""

FLAGGED_LOCATIONS = """
SELECT f.txn_lat AS lat, f.txn_lon AS lon, f.risk_level, f.amount_usd, f.fraud_score,
       array_to_string(f.reason_codes, ', ') AS reasons
FROM dw.fact_transactions f
WHERE f.is_flagged
  AND f.event_ts >= now() - %(window)s::interval
ORDER BY f.event_ts DESC
LIMIT 3000
"""

PIPELINE_HEALTH = """
WITH latest AS (
    SELECT DISTINCT ON (query_name)
           query_name, progress_ts, watermark, state_rows_total, max_offsets_behind_latest
    FROM ops.streaming_query_progress
    ORDER BY query_name, progress_ts DESC
)
SELECT p.query_name,
       count(*) FILTER (WHERE p.num_input_rows > 0)                          AS batches,
       sum(p.num_input_rows)                                                 AS rows_in,
       round(avg(p.processed_rows_per_second) FILTER (WHERE p.num_input_rows > 0)::numeric, 0) AS avg_rows_per_s,
       percentile_cont(0.50) WITHIN GROUP (ORDER BY p.batch_duration_ms)
           FILTER (WHERE p.num_input_rows > 0)                               AS p50_batch_ms,
       percentile_cont(0.95) WITHIN GROUP (ORDER BY p.batch_duration_ms)
           FILTER (WHERE p.num_input_rows > 0)                               AS p95_batch_ms,
       coalesce(sum(p.rows_dropped_by_watermark), 0)                         AS dropped_late_rows,
       l.max_offsets_behind_latest                                           AS kafka_lag,
       l.state_rows_total,
       l.watermark,
       l.progress_ts                                                         AS last_progress
FROM ops.streaming_query_progress p
JOIN latest l USING (query_name)
WHERE p.progress_ts >= now() - %(window)s::interval
GROUP BY p.query_name, l.max_offsets_behind_latest, l.state_rows_total, l.watermark, l.progress_ts
ORDER BY p.query_name
"""

ALL_QUERIES = {
    "kpi_summary": KPI_SUMMARY,
    "throughput": THROUGHPUT,
    "end_to_end_latency": END_TO_END_LATENCY,
    "micro_batch_latency": MICRO_BATCH_LATENCY,
    "timeseries": TIMESERIES,
    "alert_feed": ALERT_FEED,
    "category_risk_heatmap": CATEGORY_RISK_HEATMAP,
    "rule_performance": RULE_PERFORMANCE,
    "model_confusion": MODEL_CONFUSION,
    "merchant_spikes": MERCHANT_SPIKES,
    "flagged_locations": FLAGGED_LOCATIONS,
    "pipeline_health": PIPELINE_HEALTH,
}
