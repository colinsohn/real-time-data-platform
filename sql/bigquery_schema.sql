-- BigQuery Partitioned & Clustered Core Events Table
CREATE TABLE IF NOT EXISTS `portfolio-analytics-prod.realtime_telemetry.core_telemetry_events` (
  event_id STRING NOT NULL,
  schema_version STRING NOT NULL,
  tenant_id STRING NOT NULL,
  event_type STRING NOT NULL,
  event_ts TIMESTAMP NOT NULL,
  event_ts_ms INT64 NOT NULL,
  event_date DATE NOT NULL,
  event_hour_utc INT64 NOT NULL,
  session_id STRING NOT NULL,
  amount_cents INT64 NOT NULL,
  latency_ms FLOAT64 NOT NULL,
  attributes_json JSON,
  kafka_topic STRING NOT NULL,
  kafka_partition INT64 NOT NULL,
  kafka_offset INT64 NOT NULL,
  batch_id STRING NOT NULL
)
PARTITION BY event_date
CLUSTER BY tenant_id, event_type
OPTIONS (
  require_partition_filter = TRUE,
  description = 'Deduplicated streaming telemetry events keyed by event_id'
);

-- BigQuery Incremental Hourly Tenant Metrics Mart
CREATE TABLE IF NOT EXISTS `portfolio-analytics-prod.realtime_telemetry.mart_tenant_hourly_metrics` (
  event_date DATE NOT NULL,
  event_hour_utc INT64 NOT NULL,
  tenant_id STRING NOT NULL,
  event_type STRING NOT NULL,
  event_count INT64 NOT NULL,
  distinct_sessions INT64 NOT NULL,
  total_amount_cents INT64 NOT NULL,
  avg_latency_ms FLOAT64 NOT NULL,
  max_latency_ms FLOAT64 NOT NULL,
  min_event_ts STRING NOT NULL,
  max_event_ts STRING NOT NULL
)
PARTITION BY event_date
CLUSTER BY tenant_id, event_type;

-- Idempotent At-Least-Once Deduplicating MERGE into BigQuery Core Table
MERGE INTO `portfolio-analytics-prod.realtime_telemetry.core_telemetry_events` AS target
USING (
  SELECT * EXCEPT(row_num)
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY event_id
        ORDER BY kafka_partition ASC, kafka_offset ASC
      ) AS row_num
    FROM `portfolio-analytics-prod.realtime_telemetry.stg_telemetry_events`
  )
  WHERE row_num = 1
) AS source
ON target.event_id = source.event_id
   AND target.event_date = source.event_date
WHEN NOT MATCHED THEN
  INSERT (
    event_id, schema_version, tenant_id, event_type, event_ts, event_ts_ms,
    event_date, event_hour_utc, session_id, amount_cents, latency_ms,
    attributes_json, kafka_topic, kafka_partition, kafka_offset, batch_id
  )
  VALUES (
    source.event_id, source.schema_version, source.tenant_id, source.event_type,
    source.event_ts, source.event_ts_ms, source.event_date, source.event_hour_utc,
    source.session_id, source.amount_cents, source.latency_ms,
    source.attributes_json, source.kafka_topic, source.kafka_partition,
    source.kafka_offset, source.batch_id
  );
