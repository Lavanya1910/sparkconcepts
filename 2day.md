# Data Architecture Studio Part 2A — Data Engineering Scenario-Based Interview Prep Guide

Real-world challenges, production-grade solutions, and battle-tested code snippets for fraud detection, migration, lineage, DRP, lakehouses, monitoring, governance, load balancing, SCDs, and archiving.

## Table of Contents

1. [Building a Real-Time Fraud Detection System](#7-building-a-real-time-fraud-detection-system)
2. [Migrating On-Premises Data to the Cloud](#8-migrating-on-premises-data-to-the-cloud)
3. [Implementing Data Lineage](#9-implementing-data-lineage)
4. [Building a Disaster Recovery Plan (DRP)](#10-building-a-disaster-recovery-plan-drp)
5. [Implementing a Data Lakehouse Architecture](#11-implementing-a-data-lakehouse-architecture)
6. [Real-Time Monitoring of Data Pipelines](#12-real-time-monitoring-of-data-pipelines)
7. [Data Governance and Compliance](#13-data-governance-and-compliance)
8. [Load Balancing in Distributed Systems](#14-load-balancing-in-distributed-systems)
9. [Handling Slowly Changing Dimensions (SCD)](#15-handling-slowly-changing-dimensions-scd)
10. [Data Archiving Strategy](#16-data-archiving-strategy)

### Scenario Summary

| Scenario | Focus | Key Technologies |
| --- | --- | --- |
| 7. Real-Time Fraud Detection | Low-latency stream scoring | Kafka, Flink, ML, feature store |
| 8. Cloud Migration | Bulk data transfer, CDC, validation | Snowball, Data Box, Debezium |
| 9. Data Lineage | Metadata tracking, auditability | OpenLineage, Apache Atlas, Airflow |
| 10. Disaster Recovery Plan | RPO/RTO, failover, replication | S3 CRR, Route53, chaos engineering |
| 11. Data Lakehouse Architecture | ACID lakehouse, schema evolution | Delta Lake, Iceberg, Spark |
| 12. Monitoring Pipelines | Observability, SLOs, alerts | Prometheus, Grafana, PagerDuty |
| 13. Governance & Compliance | PII masking, audit trails | Snowflake, RLS, metadata catalog |
| 14. Load Balancing | Distributed routing, resilience | Consistent hashing, circuit breakers |
| 15. SCD Management | Historical dimension tracking | MERGE, surrogate keys, Type 2 |
| 16. Data Archiving | Cost optimization, retention | S3 lifecycle, Object Lock, Boto3 |

---

## 7. Building a Real-Time Fraud Detection System

**Focus:** Kafka, Apache Flink, ML anomaly detection, stream processing.

**Problem:** Design a real-time fraud detection system that flags fraudulent transactions within milliseconds of ingestion without disrupting genuine customers.

### Solution

- Use **Apache Kafka** for high-throughput event ingestion.
- Implement **Apache Flink** for low-latency, stateful stream processing.
- Use a feature store such as **Feast** for real-time feature lookups.
- Flag suspicious transactions asynchronously so the payment flow is not blocked.

### Real-World Example

PayPal processes about 450 transactions per second using a layered fraud detection pipeline. Their Kafka stream feeds a real-time scoring engine that evaluates hundreds of features per transaction in under 300ms. Stripe uses Flink-based scoring with hourly retrained gradient-boosted trees and a two-stage approach: a fast heuristic rules layer followed by a heavier ML model layer.

### Code Snippet — Flink Fraud Detection with Feature Enrichment

```python
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
import json
import pickle
import numpy as np

# Load pre-trained fraud model
with open("fraud_model.pkl", "rb") as f:
    model = pickle.load(f)


def score_transaction(raw_json: str) -> str:
    txn = json.loads(raw_json)
    features = np.array([
        [
            txn["amount"],
            txn["hour_of_day"],
            txn["merchant_risk_score"],
            txn["velocity_last_1h"],
            txn["is_new_device"],
            txn["geo_distance_km"],
        ]
    ])

    fraud_prob = model.predict_proba(features)[0][1]
    txn["fraud_score"] = round(fraud_prob, 4)
    txn["is_fraud"] = fraud_prob > 0.85
    txn["needs_review"] = 0.5 < fraud_prob < 0.85
    return json.dumps(txn)


env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(16)

source = KafkaSource.builder() \
    .set_bootstrap_servers("broker:9092") \
    .set_topics("raw-transactions") \
    .set_value_only_deserializer(SimpleStringSchema()) \
    .build()

stream = env.from_source(
    source,
    WatermarkStrategy.no_watermarks(),
    "Kafka Transactions"
)
scored = stream.map(score_transaction)

scored.filter(lambda x: json.loads(x)["is_fraud"]) \
    .sink_to(fraud_alerts_sink)

scored.filter(lambda x: json.loads(x)["needs_review"]) \
    .sink_to(review_queue_sink)

env.execute("FraudDetectionPipeline")
```

### Edge Cases

- **False positives:** Use a tiered scoring system (block/review/allow) rather than binary decisions.
- **High model latency:** Use vectorized scoring and model distillation on the hot path.
- **Model drift:** Retrain hourly or daily using recent labeled fraud data.

### Approach to Overcome

- Retrain models regularly with MLflow-tracked datasets.
- Use ONNX export for fast inference.
- Implement a human-in-the-loop review queue for medium-confidence scores.
- Monitor false positive rate alongside fraud catch rate.

### Expert Tip

For mission-critical fraud systems, provision at least **30% headroom** in the stream processor to prevent backpressure during peak windows such as Black Friday. Pre-warm model caches during traffic ramps.

---

## 8. Migrating On-Premises Data to the Cloud

**Focus:** AWS Snowball, CDC, checksum validation, zero-downtime migration.

**Problem:** Migrate petabytes of data from on-premises storage to the cloud with limited network bandwidth and no more than four hours of read-only mode.

### Solution

- Use **AWS Snowball** or **Azure Data Box** for bulk transfer of historical data.
- Implement **Change Data Capture (CDC)** with Debezium for delta sync.
- Apply CDC deltas after the bulk transfer completes.
- Validate data using checksums.

### Real-World Example

Capital One migrated 30PB to AWS using Snowball Edge for the initial bulk load and Debezium for continuous delta capture, achieving a final cutover window under two hours. Samsung used Azure Data Box and verified every file with SHA-256 checksums.

### Code Snippet — Debezium CDC Configuration + Checksum Validation

```json
{
  "name": "postgres-cdc-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "on-prem-db.internal",
    "database.port": "5432",
    "database.user": "cdc_user",
    "database.password": "${env:DB_PASSWORD}",
    "database.dbname": "prod_db",
    "slot.name": "debezium_slot",
    "plugin.name": "pgoutput",
    "table.include.list": "public.orders,public.customers",
    "snapshot.mode": "initial",
    "topic.prefix": "migration",
    "transforms": "unwrap",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState"
  }
}
```

```python
import hashlib
import boto3
import psycopg2


def checksum_table(conn, table: str) -> str:
    """Compute MD5 hash of entire table content (order-independent)."""
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT MD5(string_agg(t::text, '' ORDER BY id)) FROM {table} t"
        )
        return cur.fetchone()[0]


source_hash = checksum_table(on_prem_conn, "public.orders")
target_hash = checksum_table(cloud_conn, "public.orders")

if source_hash == target_hash:
    print("✅ Migration validated — checksums match!")
else:
    raise ValueError(
        f"❌ Checksum mismatch! Source={source_hash}, Target={target_hash}"
    )
```

### Edge Cases

- **Network constraints:** Use physical appliances for bulk transfer and CDC for deltas.
- **Data consistency:** Use CDC log sequence numbers as idempotency keys.
- **Schema differences:** Validate schema compatibility before migration.

### Approach to Overcome

- Use parallel multipart transfers and compression.
- Validate using MD5/SHA-256 checksums.
- Operate in dual-write mode during cutover.
- Prepare rollback procedures for DNS and connection-string switchback.

### Expert Tip

Checksum validation is the strongest guarantee of data fidelity after a physical transfer. Record counts are not enough; two rows can swap values and still produce the same count.

---

## 9. Implementing Data Lineage

**Focus:** Apache Atlas, OpenLineage, AWS Glue Data Catalog, audit compliance.

**Problem:** Provide automated, always-current lineage from source databases through transformations to dashboards.

### Solution

- Use **OpenLineage** with Apache Atlas, Marquez, or DataHub.
- Instrument Spark and Airflow jobs to emit lineage events automatically.
- Store lineage metadata in a queryable store for auditors and impact analysis.

### Real-World Example

WeWork used Apache Atlas to satisfy SOX compliance, tracing a broken join back to a dbt transformation in 30 minutes. LinkedIn's DataHub tracks lineage for 100,000+ datasets and enables impact analysis for downstream dashboards.

### Code Snippet — OpenLineage with Airflow + Spark

```python
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
import requests

with DAG(
    "sales_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    tags=["lineage", "sales"],
) as dag:

    transform_job = SparkSubmitOperator(
        task_id="transform_sales",
        application="s3://scripts/transform_sales.py",
        conf={
            "spark.extraListeners": "io.openlineage.spark.agent.OpenLineageSparkListener",
            "spark.openlineage.transport.url": "http://marquez:5000/api/v1/lineage",
            "spark.openlineage.namespace": "production",
        },
    )


def get_upstream_datasets(dataset_name: str):
    """Find all upstream sources for a given dataset."""
    resp = requests.get(
        "http://marquez:5000/api/v1/lineage",
        params={"nodeId": f"dataset:production:{dataset_name}", "depth": 5},
    )
    return resp.json()


lineage = get_upstream_datasets("gold.sales_summary")
print("Upstream nodes:", [n["id"] for n in lineage["graph"]])
```

### Edge Cases

- **Manual lineage gaps:** Automate metadata extraction to eliminate manual updates.
- **Performance overhead:** Selectively track lineage for critical datasets.
- **Cross-platform lineage:** Use OpenLineage to bridge Spark, dbt, and BI tools.

### Approach to Overcome

- Automate lineage listeners in Spark and Airflow.
- Apply selective tracking to certification-level data.
- Expose lineage through a catalog UI for self-service discovery.

### Expert Tip

Automated lineage is essential. Manual updates become outdated quickly, and a lineage gap means you cannot trace data issues back to their source when auditors ask.

---

## 10. Building a Disaster Recovery Plan (DRP)

**Focus:** RPO, RTO, multi-region replication, failover testing.

**Problem:** Define RPO/RTO targets, replication strategy, and validation without causing a real outage.

### Solution

- Implement **multi-region replication** with asynchronous replication for cost-performance balance.
- Define **RPO** and **RTO** aligned with business SLAs.
- Use automated failover with Route53 or Azure Traffic Manager.
- Test failover regularly with chaos engineering drills.

### Real-World Example

Netflix uses Chaos Monkey to validate DR plans. They maintain near-zero RPO for critical services and RTO under 30 seconds for streaming systems. AWS S3 cross-region replication is used by many companies to achieve minute-level RPO for data lakes.

### Code Snippet — Multi-Region S3 Replication + Failover (Terraform)

```hcl
resource "aws_s3_bucket_replication_configuration" "dr_replication" {
  bucket = aws_s3_bucket.primary.id
  role   = aws_iam_role.replication_role.arn

  rule {
    id     = "replicate-all"
    status = "Enabled"

    destination {
      bucket         = aws_s3_bucket.dr_region.arn
      storage_class  = "STANDARD_IA"

      encryption_configuration {
        replica_kms_key_id = aws_kms_key.dr_key.arn
      }
    }

    delete_marker_replication {
      status = "Enabled"
    }
  }
}
```

```python
import time
import boto3
import logging


def execute_dr_failover(primary_region: str, dr_region: str) -> float:
    start = time.time()
    logging.info("⚠️ DR failover initiated")

    r53 = boto3.client("route53")
    r53.change_resource_record_sets(
        HostedZoneId="YOUR_ZONE_ID",
        ChangeBatch={
            "Changes": [
                {
                    "Action": "UPSERT",
                    "ResourceRecordSet": {
                        "Name": "data-api.internal",
                        "Type": "CNAME",
                        "TTL": 30,
                        "ResourceRecords": [
                            {"Value": f"dr-endpoint.{dr_region}.internal"}
                        ],
                    },
                }
            ]
        },
    )

    elapsed = time.time() - start
    logging.info(f"✅ DNS failover complete in {elapsed:.1f}s (RTO target: 3600s)")
    return elapsed
```

### Edge Cases

- **Replication lag:** Asynchronous replication creates a measurable data loss window.
- **Corruption propagation:** Replication can copy corrupted data.
- **Untested DRP:** A plan that is never exercised may fail in a real outage.

### Approach to Overcome

- Use async replication for cost-effectiveness, synchronous for critical logs.
- Run quarterly failover drills and measure actual RTO.
- Implement point-in-time restore separate from replication.
- Use AWS Elastic Disaster Recovery or Azure Site Recovery for low RTO.

### Expert Tip

Testing failover procedures regularly is the only way to ensure a DRP works under real conditions. An untested plan gives false confidence.

---

## 11. Implementing a Data Lakehouse Architecture

**Focus:** Delta Lake, Apache Iceberg, ACID transactions, schema evolution.

**Problem:** Combine data lake flexibility with warehouse reliability while handling schema drift and query staleness.

### Solution

- Use **Delta Lake** or **Apache Iceberg** for ACID transactions and schema enforcement.
- Implement a **medallion architecture**: Bronze, Silver, Gold.
- Use incremental refreshes and metadata caching.
- Optimize with Z-order indexing and partition pruning.

### Real-World Example

Databricks runs Delta Lake on AWS S3 for streaming and batch workloads on the same tables. Apple uses Apache Iceberg at exabyte scale with hidden partitioning and time travel for auditability. Both eliminate the separate lake and warehouse architecture.

### Code Snippet — Delta Lake Medallion Architecture

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from delta.tables import DeltaTable

spark = SparkSession.builder \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Bronze: raw ingest, no transformation
raw = spark.readStream.format("kafka") \
    .option("subscribe", "orders") \
    .load()

raw.writeStream.format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "s3://lake/checkpoints/bronze_orders") \
    .start("s3://lake/bronze/orders")

# Silver: cleansed and deduplicated
bronze = spark.read.format("delta").load("s3://lake/bronze/orders")
silver = bronze \
    .dropDuplicates(["order_id"]) \
    .filter(col("amount") > 0) \
    .withColumn("ingested_at", current_timestamp())

silver.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .partitionBy("year", "month") \
    .save("s3://lake/silver/orders")

# Gold: business aggregates for BI
gold = silver.groupBy("customer_id", "year", "month") \
    .agg({"amount": "sum", "order_id": "count"})

gold.write.format("delta").mode("overwrite").save("s3://lake/gold/customer_monthly")

# Optimize Gold table
spark.sql("OPTIMIZE delta.`s3://lake/gold/customer_monthly` ZORDER BY (customer_id)")

# Time travel comparison
yesterday = spark.read.format("delta") \
    .option("versionAsOf", 10) \
    .load("s3://lake/gold/customer_monthly")
```

### Edge Cases

- **Schema drift:** Use `mergeSchema=true` and schema evolution rules.
- **Metadata lag:** Large tables with many files require caching and pruning.
- **Small files:** Run `OPTIMIZE` and `VACUUM` regularly.

### Approach to Overcome

- Use Delta/Iceberg schema evolution features.
- Schedule nightly compaction jobs.
- Refresh Gold data incrementally with `MERGE INTO`.
- Enable result caching for frequent queries.

### Expert Tip

ACID transactions and time travel are the key value propositions of a lakehouse. They make streaming writes and analytics consistent and debuggable.

---

## 12. Real-Time Monitoring of Data Pipelines

**Focus:** Prometheus, Grafana, alerting, SLOs, observability.

**Problem:** Implement monitoring and alerting for critical pipelines that process tens of millions in daily transactions.

### Solution

- Use **Prometheus** for metrics collection and **Grafana** for dashboards.
- Instrument pipelines with metrics for lag, throughput, errors, and freshness.
- Define SLOs and multi-tier alerts.
- Use PagerDuty for escalation.

### Real-World Example

Lyft tracks hundreds of metrics per pipeline and enforces an SLO that 99.9% of daily tables are available by 8 AM ET. Airbnb monitors freshness, row counts, and column statistics for certified datasets.

### Code Snippet — Custom Prometheus Metrics + Grafana Alerts

```python
from prometheus_client import Counter, Histogram, Gauge, start_http_server
import time
import functools

PIPELINE_ROWS = Counter(
    "pipeline_rows_total",
    "Total rows processed",
    ["pipeline", "status"],
)
PIPELINE_LATENCY = Histogram(
    "pipeline_duration_seconds",
    "Pipeline run duration",
    ["pipeline"],
    buckets=[60, 300, 600, 1800, 3600],
)
DATA_FRESHNESS = Gauge(
    "data_freshness_seconds",
    "Seconds since last successful load",
    ["table"],
)
KAFKA_LAG = Gauge(
    "kafka_consumer_lag",
    "Current Kafka consumer lag",
    ["topic", "group"],
)


def monitor_pipeline(pipeline_name: str):
    """Decorator to add timing and error tracking to any pipeline function."""

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start = time.time()
            try:
                result = func(*args, **kwargs)
                PIPELINE_ROWS.labels(pipeline_name, "success").inc(
                    result.get("rows", 0)
                )
                return result
            except Exception:
                PIPELINE_ROWS.labels(pipeline_name, "error").inc()
                raise
            finally:
                PIPELINE_LATENCY.labels(pipeline_name).observe(
                    time.time() - start
                )

        return wrapper

    return decorator
```

```yaml
# Grafana alert rule example
alert: KafkaConsumerLagHigh
expr: kafka_consumer_lag{topic="orders"} > 50000
for: 5m
labels:
  severity: critical
annotations:
  summary: "Kafka consumer lag is too high for orders topic"
  description: "Trigger on-call when lag exceeds 50,000 for 5 minutes."
```

### Edge Cases

- **False alarms:** Use `for: 5m` to avoid transient spikes.
- **High-cardinality metrics:** Aggregate to cohort or segment level.
- **Alert fatigue:** Use severity tiers and suppression rules.

### Approach to Overcome

- Group and suppress correlated alerts.
- Use Prometheus recording rules for expensive queries.
- Define SLO burn rate alerts.
- Review incidents and tune thresholds.

### Expert Tip

Each incident should yield at least one monitoring improvement, such as a new alert, dashboard panel, or automated runbook.

---

## 13. Data Governance and Compliance

**Focus:** GDPR, CCPA, data masking, audit trails, row-level security.

**Problem:** Comply with GDPR and CCPA across 50+ tables containing PII.

### Solution

- Implement column-level masking and encryption.
- Maintain audit trails for all data access.
- Use row-level security and policy-based access control.
- Tag PII columns and automate erasure workflows.

### Real-World Example

Booking.com tags PII with ML classifiers and enforces masking via Apache Ranger. Salesforce propagates deletion requests across 27 downstream systems within 30 days.

### Code Snippet — Dynamic Data Masking in Snowflake

```sql
CREATE OR REPLACE MASKING POLICY email_mask
AS (val STRING) RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('PII_ADMIN', 'COMPLIANCE_ROLE') THEN val
    ELSE REGEXP_REPLACE(val, '.+@', '*****@')
  END;

ALTER TABLE customers MODIFY COLUMN email
  SET MASKING POLICY email_mask;
```

```sql
SELECT
  query_id,
  user_name,
  role_name,
  query_text,
  start_time,
  rows_returned
FROM snowflake.account_usage.access_history
WHERE ARRAY_CONTAINS('customers'::variant, direct_objects_accessed)
  AND start_time > DATEADD('day', -30, CURRENT_TIMESTAMP())
ORDER BY start_time DESC;
```

```sql
UPDATE customers
SET
  email = 'deleted_user@gdpr.invalid',
  first_name = 'DELETED',
  last_name = 'DELETED',
  phone = '000-000-0000',
  address = 'REDACTED',
  deleted_at = CURRENT_TIMESTAMP()
WHERE customer_id = 'USR-99887';
```

### Edge Cases

- **Masking performance:** Use masked views for large scans.
- **Coarse roles:** Enforce column-level and row-level controls.
- **Unstructured PII:** Use NLP-based scanners for PDFs and JSON blobs.

### Approach to Overcome

- Use row-level security driven by session context.
- Scan for PII with AWS Macie or Google DLP.
- Automate GDPR/CCPA requests through metadata-driven pipelines.

### Expert Tip

Centralized metadata and audit logging are the pillars of governance. Tag all sensitive columns so you can manage them consistently.

---

## 14. Load Balancing in Distributed Systems

**Focus:** Consistent hashing, auto-scaling, rate limiting, circuit breakers.

**Problem:** Evenly distribute workload across nodes and protect downstream systems during spikes.

### Solution

- Use **consistent hashing** to distribute partition keys.
- Scale horizontally with auto-scaling groups or Kubernetes HPA.
- Apply rate limiting and circuit breakers.

### Real-World Example

Amazon DynamoDB uses consistent hashing for trillions of requests. Discord uses similar routing to keep a 100x traffic spike from impacting other users.

### Code Snippet — Consistent Hashing + Circuit Breaker

```python
import hashlib
from sortedcontainers import SortedDict
from collections import defaultdict
import requests
from circuitbreaker import circuit


class ConsistentHashRing:
    """Route data partitions to worker nodes using consistent hashing."""

    def __init__(self, nodes: list[str], virtual_nodes: int = 150):
        self.ring = SortedDict()
        self.vnodes = virtual_nodes
        self.node_loads = defaultdict(int)
        for node in nodes:
            self.add_node(node)

    def add_node(self, node: str):
        for i in range(self.vnodes):
            key = hashlib.md5(f"{node}:{i}".encode()).hexdigest()
            self.ring[key] = node

    def get_node(self, partition_key: str) -> str:
        h = hashlib.md5(partition_key.encode()).hexdigest()
        idx = self.ring.bisect_left(h)
        if idx == len(self.ring):
            idx = 0
        node = self.ring.values()[idx]
        self.node_loads[node] += 1
        return node


@circuit(failure_threshold=5, recovery_timeout=30)
def call_downstream_api(payload):
    response = requests.post("http://downstream-service/process", json=payload)
    response.raise_for_status()
    return response.json()


ring = ConsistentHashRing(["worker-1", "worker-2", "worker-3", "worker-4"])
node = ring.get_node("customer_id_12345")
print(f"Routed to: {node}")
```

### Edge Cases

- **Traffic spikes:** Use predictive scaling policies.
- **Health check misconfiguration:** Validate settings before production.
- **Hot partitions:** Detect and isolate hot keys.

### Approach to Overcome

- Rate limit with token bucket or leaky bucket algorithms.
- Protect external calls with circuit breakers.
- Alert on load imbalance greater than 20%.
- Use queue depth or consumer lag as leading scaling metrics.

### Expert Tip

Auto-scaling works best when paired with the right metrics. CPU alone is often a lagging signal for distributed systems.

---

## 15. Handling Slowly Changing Dimensions (SCD)

**Focus:** SCD Type 2, surrogate keys, MERGE, historical tracking.

**Problem:** Retain old and new customer address history for accurate attribution while preserving historical analytics.

### Solution

- Use **SCD Type 2** to keep a row for every change.
- Use surrogate keys for versioned records.
- Track `effective_date`, `expiry_date`, and `is_current`.
- Use `MERGE INTO` for atomic upserts.

### Real-World Example

Amazon uses SCD Type 2 for customer history, keeping past addresses for analytics while attributing new orders correctly. Walmart applies SCD Type 2 for store hierarchies during openings, closings, and reformatting.

### Code Snippet — SCD Type 2 MERGE in Snowflake

```sql
CREATE TABLE IF NOT EXISTS dim_customers (
  customer_sk NUMBER AUTOINCREMENT PRIMARY KEY,
  customer_id VARCHAR(50) NOT NULL,
  first_name VARCHAR(100),
  last_name VARCHAR(100),
  email VARCHAR(255),
  city VARCHAR(100),
  state VARCHAR(50),
  effective_date DATE NOT NULL,
  expiry_date DATE DEFAULT '9999-12-31',
  is_current BOOLEAN DEFAULT TRUE,
  row_hash VARCHAR(64)
);

MERGE INTO dim_customers tgt
USING (
  SELECT
    customer_id,
    first_name,
    last_name,
    email,
    city,
    state,
    CURRENT_DATE() AS effective_date,
    SHA2(CONCAT(city, state, email)) AS row_hash
  FROM staging_customers
) src
ON tgt.customer_id = src.customer_id
AND tgt.is_current = TRUE
WHEN MATCHED AND tgt.row_hash != src.row_hash THEN
  UPDATE SET
    tgt.expiry_date = DATEADD('day', -1, CURRENT_DATE()),
    tgt.is_current = FALSE
WHEN NOT MATCHED THEN
  INSERT (
    customer_id,
    first_name,
    last_name,
    email,
    city,
    state,
    effective_date,
    expiry_date,
    is_current,
    row_hash
  ) VALUES (
    src.customer_id,
    src.first_name,
    src.last_name,
    src.email,
    src.city,
    src.state,
    src.effective_date,
    '9999-12-31',
    TRUE,
    src.row_hash
  );

INSERT INTO dim_customers (
  customer_id,
  first_name,
  last_name,
  email,
  city,
  state,
  effective_date,
  is_current
)
SELECT
  s.customer_id,
  s.first_name,
  s.last_name,
  s.email,
  s.city,
  s.state,
  CURRENT_DATE(),
  TRUE
FROM staging_customers s
JOIN dim_customers d
  ON s.customer_id = d.customer_id
  AND NOT d.is_current;
```

### Edge Cases

- **Data bloat:** Use Type 1 for low-value attributes and Type 2 only for important changes.
- **Incorrect updates:** Use row hashing to detect changes and prevent bad merges.
- **Late-arriving updates:** Backdate effective dates for corrections.

### Approach to Overcome

- Use `MERGE` with row hash change detection.
- Archive old SCD rows beyond the retention window.
- Validate exactly one `is_current = TRUE` row per business key.
- Use Type 0 for immutable attributes and Type 3 for simple previous-value tracking.

### Expert Tip

Always join fact tables to surrogate keys, never natural business keys, for SCD Type 2. Surrogate keys decouple versioned history from mutable business identifiers.

---

## 16. Data Archiving Strategy

**Focus:** AWS Glacier, tiered storage, lifecycle policies, retention compliance.

**Problem:** Archive infrequently accessed data to reduce storage costs while maintaining regulatory retention and preventing accidental loss.

### Solution

- Implement a tiered strategy: Hot → Warm → Cold → Delete.
- Apply retention tags to each dataset.
- Use S3 Object Lock for compliance-backed immutability.
- Automate transitions with lifecycle policies.

### Real-World Example

Goldman Sachs archives trading records in Glacier Deep Archive to satisfy SEC retention and save millions. They use S3 Object Lock in Compliance mode so records cannot be deleted before retention expires.

### Code Snippet — Cost-Optimized Archive Pipeline (Python + Boto3)

```python
import boto3
from datetime import datetime, timedelta
import logging

s3 = boto3.client("s3")

RETENTION_POLICIES = {
    "financial_transactions": {
        "warm_after_days": 90,
        "cold_after_days": 365,
        "delete_after_days": 2555,
    },
    "user_clickstream": {
        "warm_after_days": 30,
        "cold_after_days": 90,
        "delete_after_days": 1825,
    },
    "raw_logs": {
        "warm_after_days": 7,
        "cold_after_days": 30,
        "delete_after_days": 365,
    },
}


def apply_lifecycle_policy(bucket: str, prefix: str, policy_name: str):
    policy = RETENTION_POLICIES[policy_name]
    s3.put_bucket_lifecycle_configuration(
        Bucket=bucket,
        LifecycleConfiguration={
            "Rules": [
                {
                    "ID": f"{policy_name}-lifecycle",
                    "Status": "Enabled",
                    "Filter": {"Prefix": prefix},
                    "Transitions": [
                        {
                            "Days": policy["warm_after_days"],
                            "StorageClass": "STANDARD_IA",
                        },
                        {
                            "Days": policy["cold_after_days"],
                            "StorageClass": "GLACIER",
                        },
                    ],
                    "Expiration": {"Days": policy["delete_after_days"]},
                    "NoncurrentVersionExpiration": {
                        "NewerNoncurrentVersions": 3,
                    },
                }
            ]
        },
    )
    logging.info(f"✅ Applied {policy_name} lifecycle to s3://{bucket}/{prefix}")


apply_lifecycle_policy("company-datalake", "financial/", "financial_transactions")
apply_lifecycle_policy("company-datalake", "clickstream/", "user_clickstream")

GB_COLD = 100_000
GB_WARM = 50_000
savings = GB_COLD * (0.023 - 0.004) + GB_WARM * (0.023 - 0.0125)
print(f"Monthly savings: ${savings:,.0f}")
```

### Edge Cases

- **Cold retrieval delay:** Glacier Standard can take 3–5 hours; use S3-IA for occasional access.
- **Accidental deletion:** Use Object Lock and versioning before lifecycle policies.
- **Compliance conflicts:** Pseudonymize PII instead of deleting financial records when retention and erasure requirements conflict.

### Approach to Overcome

- Enable S3 Versioning and Object Lock before lifecycle transitions.
- Prioritize retrieval with Glacier Expedited for critical audits.
- Review policies regularly as regulations change.
- Tag datasets with `retention_class`, `data_owner`, and `compliance_requirement`.

### Expert Tip

Tiered storage is powerful, but policies must be reviewed periodically. A two-year-old lifecycle rule may not reflect current regulatory requirements.

---

Data Architecture Studio — Data Engineering Scenario-Based Interview Prep Guide · Part 2A (Challenges 7–16)

Extended with real-world examples, production code snippets, and deep-dive solutions.

© 2026 DataArchitectStudio. For educational purposes only.
