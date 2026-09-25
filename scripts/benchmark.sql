-- Benchmark report built from the pipeline's own telemetry (ops.streaming_query_progress,
-- written by the StreamingQueryListener) and the warehouse load timestamps.
--   make benchmark            or
--   docker compose exec -T postgres psql -U fraud -d fraud_dw -f - < scripts/benchmark.sql
\pset footer off
\echo '== Per-query micro-batch performance (batches that processed data, last 15 minutes)'
SELECT query_name,
       count(*)                                                                   AS batches,
       sum(num_input_rows)                                                        AS rows_processed,
       round(avg(num_input_rows))                                                 AS avg_rows_per_batch,
       round(percentile_cont(0.50) WITHIN GROUP (ORDER BY batch_duration_ms))     AS p50_batch_ms,
       round(percentile_cont(0.95) WITHIN GROUP (ORDER BY batch_duration_ms))     AS p95_batch_ms,
       max(batch_duration_ms)                                                     AS max_batch_ms,
       round(avg(processed_rows_per_second)::numeric)                             AS avg_processing_rows_per_s,
       round(100.0 * count(*) FILTER (WHERE batch_duration_ms < 2000) / count(*), 1) AS pct_batches_under_2s,
       max(max_offsets_behind_latest)                                             AS max_kafka_lag,
       coalesce(sum(rows_dropped_by_watermark), 0)                                AS late_rows_dropped
FROM ops.streaming_query_progress
WHERE num_input_rows > 0
  AND progress_ts >= now() - interval '15 minutes'
GROUP BY query_name
ORDER BY query_name;

\echo '== Sustained ingest throughput (bronze, per minute, last 15 minutes)'
SELECT date_trunc('minute', progress_ts)             AS minute,
       sum(num_input_rows)                           AS events,
       round(sum(num_input_rows) / 60.0)             AS events_per_second
FROM ops.streaming_query_progress
WHERE query_name = 'bronze_transactions'
  AND progress_ts >= now() - interval '15 minutes'
GROUP BY 1
ORDER BY 1;

\echo '== End-to-end latency: Kafka append -> row queryable in Postgres (last 10 minutes)'
SELECT count(*)                                                                                        AS rows_loaded,
       round(percentile_cont(0.50) WITHIN GROUP (ORDER BY extract(epoch FROM loaded_at - kafka_ts))::numeric, 2) AS p50_s,
       round(percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM loaded_at - kafka_ts))::numeric, 2) AS p95_s,
       round(percentile_cont(0.99) WITHIN GROUP (ORDER BY extract(epoch FROM loaded_at - kafka_ts))::numeric, 2) AS p99_s
FROM dw.fact_transactions
WHERE event_ts >= now() - interval '40 minutes'
  AND loaded_at >= now() - interval '10 minutes';

\echo '== Warehouse totals'
SELECT (SELECT count(*) FROM dw.fact_transactions)        AS transactions,
       (SELECT count(*) FROM dw.fact_fraud_alerts)        AS alerts,
       (SELECT count(*) FROM dw.fact_chargebacks)         AS chargebacks,
       (SELECT count(*) FROM dw.agg_merchant_window_risk) AS merchant_windows;
