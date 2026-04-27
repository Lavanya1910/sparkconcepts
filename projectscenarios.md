# Data Architecture Studio Part 1 — Data Engineering Scenario-Based Interview Prep Guide

Real-world challenges, production-grade solutions, and battle-tested code snippets to help you ace your next data engineering interview.

## Table of Contents

1. [Building a Scalable Data Pipeline](#1-building-a-scalable-data-pipeline)
2. [Optimizing Slow Queries in a Data Warehouse](#2-optimizing-slow-queries-in-a-data-warehouse)
3. [Handling Data Duplication](#3-handling-data-duplication)
4. [Designing a Multi-Tenant Data Platform](#4-designing-a-multi-tenant-data-platform)
5. [Ensuring Data Quality](#5-ensuring-data-quality)
6. [Reducing Data Storage Costs](#6-reducing-data-storage-costs)

---

## 1. Building a Scalable Data Pipeline

**Focus:** Real-time ingestion, stream processing, data lake architecture.

**Problem:** Design a real-time pipeline that ingests, processes, and stores clickstream data from millions of users while handling high-velocity events reliably and at scale.

### Solution

- Use **Apache Kafka** for real-time ingestion.
- Implement **Apache Flink** or **Apache Spark Structured Streaming** for stream processing.
- Store processed data in a **data lake** such as AWS S3 and in a **columnar data warehouse** like Snowflake or Redshift.
- Apply **checkpointing** and **fault tolerance** to ensure reliability.

### Real-World Example

Netflix uses Kafka for ingesting hundreds of billions of events per day. Apache Flink processes those streams, and enriched data lands in S3 (Iceberg format) for querying via Spark or Trino. LinkedIn uses Kafka + Samza for its feed ranking pipeline.

### Code Snippet — PySpark Structured Streaming (Kafka → S3)

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window
from pyspark.sql.types import StructType, StringType, LongType, TimestampType

spark = SparkSession.builder \
    .appName("ClickstreamPipeline") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

schema = StructType() \
    .add("user_id", StringType()) \
    .add("page_url", StringType()) \
    .add("event_type", StringType()) \
    .add("timestamp", LongType())

raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker1:9092,broker2:9092") \
    .option("subscribe", "clickstream-events") \
    .option("startingOffsets", "latest") \
    .option("maxOffsetsPerTrigger", 100000) \
    .load()

events = raw_stream \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", (col("timestamp") / 1000).cast(TimestampType()))

aggregated = events \
    .withWatermark("event_time", "10 minutes") \
    .groupBy(window("event_time", "5 minutes"), "page_url") \
    .count() \
    .withColumnRenamed("count", "page_views")

query = aggregated.writeStream \
    .format("parquet") \
    .option("path", "s3://datalake/clickstream/aggregated/") \
    .option("checkpointLocation", "s3://datalake/checkpoints/clickstream/") \
    .outputMode("append") \
    .trigger(processingTime="30 seconds") \
    .start()

query.awaitTermination()
```

### Edge Cases

- **High data velocity:** Kafka backlogs require partitioning and scaling consumer parallelism.
- **Schema evolution:** Use Confluent Schema Registry with Avro or Protobuf.
- **Late-arriving data:** Apply watermarking to drop or handle records beyond the acceptable window.

### Approach to Overcome

- Monitor Kafka consumer lag with Prometheus and Grafana.
- Implement watermarking in stream processing.
- Use auto-scaling consumer groups with Kubernetes HPA or KEDA.
- Enable exactly-once semantics in Kafka + Spark using idempotent producers and transactional consumers.

### Expert Tip

Always decouple ingestion from processing with a distributed message queue like Kafka. This backpressure buffer prevents data loss and allows independent scaling of producers and consumers.

---

## 2. Optimizing Slow Queries in a Data Warehouse

**Focus:** Query tuning, partitioning, clustering, materialized views.

**Problem:** A critical dashboard query takes over 10 minutes in Snowflake or Redshift, and stakeholders need it under 30 seconds.

### Solution

- Analyze the query execution plan using `EXPLAIN`.
- Optimize joins with partitioning and clustering on filter columns.
- Use materialized views for expensive aggregations.
- Denormalize when low read latency is critical.

### Real-World Example

Uber reduced a 45-minute daily report to 2 minutes by:

- partitioning the `trips` table by `city_id` and date,
- pre-aggregating trip metrics into a materialized summary table refreshed every 15 minutes,
- converting subqueries into temp tables.

### Code Snippet — Diagnosing & Fixing in Snowflake

```sql
-- Step 1: Check the slow query's execution plan
EXPLAIN
SELECT
    o.customer_id,
    SUM(o.order_amount) AS total_revenue,
    COUNT(o.order_id) AS order_count
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
WHERE o.order_date > '2024-01-01'
  AND c.region = 'NORTH_AMERICA'
GROUP BY 1;

-- Step 2: Create an optimized clustered table
CREATE OR REPLACE TABLE orders_optimized
CLUSTER BY (order_date, customer_id)
AS SELECT * FROM orders;

-- Step 3: Build a materialized view for dashboard aggregation
CREATE OR REPLACE MATERIALIZED VIEW mv_customer_revenue AS
SELECT
    o.customer_id,
    c.region,
    DATE_TRUNC('month', o.order_date) AS order_month,
    SUM(o.order_amount) AS total_revenue,
    COUNT(o.order_id) AS order_count
FROM orders_optimized o
JOIN customers c ON o.customer_id = c.customer_id
GROUP BY 1, 2, 3;

-- Step 4: Query the materialized view for fast response
SELECT customer_id,
       SUM(total_revenue)
FROM mv_customer_revenue
WHERE region = 'NORTH_AMERICA'
  AND order_month > '2024-01-01'
GROUP BY 1
ORDER BY 2 DESC
LIMIT 100;
```

### Code Snippet — Detecting Data Skew in Spark

```python
from pyspark.sql.functions import col, count, broadcast, expr, floor, rand, lit

# Check partition sizes to detect skew
orders.groupBy(col("customer_id")) \
    .agg(count("*").alias("rows")) \
    .orderBy(col("rows").desc()) \
    .show(10)

# Fix: broadcast small dimension tables to avoid shuffle
result = orders.join(
    broadcast(customers),
    on="customer_id",
    how="inner"
)

# Fix: salt keys for heavy-skew aggregations
SALT_BUCKETS = 10
salted = orders \
    .withColumn("salt", (rand() * SALT_BUCKETS).cast("int")) \
    .withColumn("salted_key", expr("concat(customer_id, '_', salt)"))

partial = salted.groupBy("salted_key", "customer_id").sum("order_amount")
final = partial.groupBy("customer_id").sum("sum(order_amount)")
```

### Edge Cases

- **Skewed data distribution:** A few customer IDs can cause uneven task durations.
- **Cross-region joins:** Different cloud regions add latency and egress cost.
- **Materialized view staleness:** Refresh lag can surface stale dashboard results.

### Approach to Overcome

- Use bucketing to pre-group data and reduce join skew.
- Enable query result caching in Snowflake or Redshift.
- Ensure queries access data within the same cloud region.
- Review the Snowflake query profile to identify expensive operations.

### Expert Tip

Prioritize partitioning on columns most frequently used in `WHERE` filters, such as date or region. This often reduces I/O overhead by 80–90%.

---

## 3. Handling Data Duplication

**Focus:** Idempotent processing, deduplication, exactly-once semantics.

**Problem:** Duplicate records appear in the data warehouse due to job reruns or reprocessing of historical data.

### Solution

- Use unique composite keys for idempotency.
- Deduplicate during ingestion using primary keys or upserts.
- In Delta Lake or Iceberg, use `MERGE INTO` for atomic upserts.

### Real-World Example

Airbnb prevents duplicates by storing an `idempotency_key` (a SHA-256 hash of event content) in Redis and checking it before inserting. Delta Lake tables use `MERGE INTO` to guarantee exactly-once writes when jobs retry.

### Code Snippet — Delta Lake Merge (Upsert) for Deduplication

```python
from delta.tables import DeltaTable
from pyspark.sql.functions import sha2, concat_ws, col, current_timestamp

incoming = spark.read.parquet("s3://landing/orders/2024-01-15/")

incoming = incoming.withColumn(
    "idempotency_key",
    sha2(concat_ws("|", col("order_id"), col("event_type"), col("event_ts")), 256)
)

# Deduplicate within the incoming micro-batch first
 deduped_batch = incoming.dropDuplicates(["idempotency_key"])

delta_table = DeltaTable.forPath(spark, "s3://datalake/orders/")

delta_table.alias("target").merge(
    deduped_batch.alias("source"),
    "target.idempotency_key = source.idempotency_key"
).whenNotMatchedInsertAll() \
 .execute()

# For updates: handle late corrections
 delta_table.alias("target").merge(
    deduped_batch.alias("source"),
    "target.order_id = source.order_id"
).whenMatchedUpdate(
    condition="source.updated_at > target.updated_at",
    set={
        "status": "source.status",
        "amount": "source.amount"
    }
).whenNotMatchedInsertAll() \
 .execute()
```

### Code Snippet — Bloom Filter for Efficient Duplicate Detection

```python
from pybloom_live import BloomFilter
import hashlib

bf = BloomFilter(capacity=1_000_000, error_rate=0.001)

def is_duplicate(record: dict) -> bool:
    key = hashlib.sha256(
        f"{record['order_id']}|{record['event_type']}|{record['event_ts']}".encode()
    ).hexdigest()

    if key in bf:
        return True

    bf.add(key)
    return False

new_records = [r for r in incoming_records if not is_duplicate(r)]
print(f"Filtered {len(incoming_records) - len(new_records)} duplicates")
```

### Edge Cases

- **Race conditions:** Concurrent jobs may both treat a record as new. Use distributed locks or database-level upserts.
- **High-cardinality datasets:** Storing every key in memory is expensive.
- **Late corrections:** Corrected records can arrive after the original event.

### Approach to Overcome

- Use distributed locks with Redis `SETNX` or ZooKeeper.
- Apply Bloom filters for memory-efficient duplicate detection.
- Design ETL jobs to be idempotent: the same input should always produce the same output.
- Add a unique constraint on the idempotency key as a safety net.

### Expert Tip

Design processing logic to be idempotent so rerunning a job with the same input produces the same output without duplicates. This turns data failures into retryable scenarios.

---

## 4. Designing a Multi-Tenant Data Platform

**Focus:** RBAC, namespace isolation, resource quotas, encryption.

**Problem:** Build a Kubernetes-based multi-tenant data platform that isolates customer data while sharing infrastructure.

### Solution

- Use one Kubernetes namespace per tenant.
- Implement Role-Based Access Control (RBAC) for security.
- Apply resource quotas to cap CPU and memory per tenant.
- Tag data with `tenant_id` and enforce row-level security.

### Real-World Example

Databricks isolates enterprise customers with dedicated compute clusters while sharing storage. AWS Lake Formation provides row-level and column-level security so analytics queries automatically filter tenant data. Snowflake isolates compute with Virtual Warehouses while sharing storage.

### Code Snippet — Kubernetes Namespace Isolation + Resource Quotas

```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: tenant-acme
  labels:
    tenant: acme
    environment: production
---
apiVersion: v1
kind: ResourceQuota
metadata:
  name: acme-quota
  namespace: tenant-acme
spec:
  hard:
    requests.cpu: "8"
    requests.memory: 16Gi
    limits.cpu: "16"
    limits.memory: 32Gi
    pods: "50"
    persistentvolumeclaims: "10"
---
apiVersion: v1
kind: LimitRange
metadata:
  name: acme-limits
  namespace: tenant-acme
spec:
  limits:
    - default:
        cpu: "500m"
        memory: 512Mi
      defaultRequest:
        cpu: "250m"
        memory: 256Mi
      type: Container
```

### Code Snippet — Row-Level Security in Snowflake

```sql
CREATE OR REPLACE ROW ACCESS POLICY tenant_isolation_policy
AS (tenant_id VARCHAR) RETURNS BOOLEAN ->
  CASE
    WHEN IS_ROLE_IN_SESSION('ADMIN_ROLE') THEN TRUE
    WHEN tenant_id = CURRENT_ROLE() THEN TRUE
    ELSE FALSE
  END;

ALTER TABLE orders
  ADD ROW ACCESS POLICY tenant_isolation_policy ON (tenant_id);
```

### Code Snippet — PySpark Tenant Enforcement

```python
from pyspark.sql.functions import col

def get_tenant_data(spark, table: str, tenant_id: str):
    """Always inject tenant_id predicate — never trust client-side filtering."""
    return spark.table(table).filter(col("tenant_id") == tenant_id)
```

### Edge Cases

- **Noisy neighbor effect:** A tenant running heavy batch jobs can starve resources.
- **Data leakage risks:** Misconfigured RBAC or row-level policies can expose tenant data.
- **Cross-tenant joins:** Bugs may accidentally join data across tenants.

### Approach to Overcome

- Enforce ResourceQuotas and LimitRanges per namespace.
- Use network policies to prevent cross-tenant pod communication.
- Implement encryption at rest and in transit.
- Tag assets with `tenant_id` and enforce checks with policy-as-code tools like OPA or Gatekeeper.

### Expert Tip

Enforce physical resource limits alongside logical isolation. Real security comes from namespace isolation, RBAC, row-level policies, and audit logging.

---

## 5. Ensuring Data Quality

**Focus:** Data quality checks, anomaly detection, CI/CD validation.

**Problem:** Dashboards show inaccurate or missing metrics, causing stakeholder distrust.

### Solution

- Implement automated checks with Great Expectations or dbt tests.
- Define SLAs for accuracy, completeness, and freshness.
- Integrate quality gates into CI/CD pipelines.

### Real-World Example

Spotify runs over 10,000 automated data quality checks daily with Great Expectations. Each data product has a freshness SLA, and failed checks trigger alerts before dashboards update. Twitter (X) uses dbt tests to prevent schema-breaking changes from reaching production.

### Code Snippet — Great Expectations Data Quality Suite

```python
import great_expectations as ge
import pandas as pd

df = pd.read_parquet("s3://datalake/orders/2024-01-15/")
ge_df = ge.from_pandas(df)

ge_df.expect_column_to_exist("order_id")
ge_df.expect_column_values_to_not_be_null("order_id")
ge_df.expect_column_values_to_be_unique("order_id")
ge_df.expect_column_values_to_not_be_null("customer_id")
ge_df.expect_column_values_to_be_between(
    "order_amount",
    min_value=0.01,
    max_value=50000.00,
)
ge_df.expect_column_values_to_be_in_set(
    "status",
    ["PENDING", "CONFIRMED", "SHIPPED", "CANCELLED"],
)
ge_df.expect_column_values_to_match_regex(
    "email",
    r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$",
)
ge_df.expect_column_values_to_not_be_null("phone", mostly=0.95)

results = ge_df.validate()
if not results["success"]:
    failed = [r for r in results["results"] if not r["success"]]
    raise ValueError(f"Data quality failed: {len(failed)} checks failed")

print("✅ All data quality checks passed!")
```

### Code Snippet — dbt Schema Test (`schema.yml`)

```yaml
version: 2
models:
  - name: stg_orders
    description: "Staged orders from transactional DB"
    columns:
      - name: order_id
        description: "Primary key for orders"
        tests:
          - unique
          - not_null
      - name: customer_id
        tests:
          - not_null
          - relationships:
              to: ref('stg_customers')
              field: customer_id
      - name: status
        tests:
          - accepted_values:
              values: ['pending', 'confirmed', 'shipped', 'cancelled']
      - name: order_amount
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: "> 0"
```

### Edge Cases

- **Upstream schema changes:** Column renames or type changes break pipelines.
- **Anomalous data spikes:** Legitimate spikes may be flagged as errors.
- **Silent data corruption:** Values may look valid but be semantically wrong.

### Approach to Overcome

- Use anomaly detection models such as Isolation Forest or Z-score.
- Add schema validation to CI/CD pipelines.
- Build a data observability layer for freshness, volume, and drift.
- Define and enforce data contracts between producer and consumer teams.

### Expert Tip

Shift left by integrating quality checks into transformation layers, not just reporting. Catching issues in staging is far cheaper than correcting bad production metrics.

---

## 6. Reducing Data Storage Costs

**Focus:** Tiered storage, columnar formats, lifecycle policies, compression.

**Problem:** Cloud storage costs have tripled because raw JSON logs are stored alongside processed Parquet files and no data archiving is in place.

### Solution

- Implement **tiered storage** with hot/warm/cold policies.
- Convert CSV/JSON to **Parquet** or **ORC**.
- Partition large datasets by date.
- Apply compression such as **ZSTD**.

### Real-World Example

Pinterest cut S3 costs by 40% by converting raw JSON logs to Parquet and applying Zstandard compression. They also moved data older than 90 days to S3 Standard-IA and older than 365 days to Glacier. Shopify saved $2M/year with a hot/warm/cold data tiering strategy.

### Code Snippet — Convert JSON to Parquet with Partitioning (PySpark)

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth

spark = SparkSession.builder \
    .appName("JSONToParquetConverter") \
    .config("spark.sql.parquet.compression.codec", "zstd") \
    .config("spark.sql.parquet.block.size", "134217728") \
    .getOrCreate()

raw_json = spark.read \
    .option("inferSchema", "true") \
    .json("s3://raw-data/events/2024/**/*.json")

print(f"Input rows: {raw_json.count():,}")
print(f"Input size: ~{raw_json.rdd.map(lambda r: len(str(r))).sum() / 1e9:.2f} GB")

partitioned = raw_json \
    .withColumn("year", year(col("event_time"))) \
    .withColumn("month", month(col("event_time"))) \
    .withColumn("day", dayofmonth(col("event_time")))

partitioned.write \
    .mode("overwrite") \
    .partitionBy("year", "month", "day") \
    .parquet("s3://optimized-datalake/events/")
```

### Code Snippet — S3 Lifecycle Policy (Terraform)

```hcl
resource "aws_s3_bucket_lifecycle_configuration" "datalake_lifecycle" {
  bucket = aws_s3_bucket.datalake.id

  rule {
    id     = "raw-data-tiering"
    status = "Enabled"

    filter {
      prefix = "raw/"
    }

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = 90
      storage_class = "GLACIER"
    }

    expiration {
      days = 2555
    }
  }

  rule {
    id     = "processed-data-tiering"
    status = "Enabled"

    filter {
      prefix = "processed/"
    }

    transition {
      days          = 90
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = 365
      storage_class = "GLACIER"
    }
  }
}
```

### Edge Cases

- **Cold data retrieval delays:** Glacier restores can take hours; use S3-IA for occasional access.
- **Data loss risk:** Aggressive retention policies may delete required data.
- **Small file problem:** Many tiny Parquet files add Spark metadata overhead.

### Approach to Overcome

- Automate tiering with S3 lifecycle policies.
- Implement versioning and backups for critical datasets.
- Run periodic Delta `OPTIMIZE` and `VACUUM` jobs to compact files.
- Use cost attribution tools to tag storage usage by team or domain.

### Expert Tip

Use columnar formats with ZSTD compression and date partitioning to reduce storage costs and improve query performance. This combination often delivers 90%+ improvement compared to raw JSON or CSV.

---

Data Architecture Studio — Data Engineering Scenario-Based Interview Prep Guide · Part 1 (Challenges 1–6)

Extended with real-world examples, production code snippets, and deep-dive solutions.

© 2026 DataArchitectStudio. For educational purposes only.
