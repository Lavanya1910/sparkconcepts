# 15. What Optimisations Did You Perform in the Project?

## Very Detailed Senior-Level Explanation

---

# What Interviewer Is Actually Testing

When interviewer asks:

**“What optimisations did you perform?”**

They are not asking:

* Did you change one config?
* Did you tune one Spark parameter?

They are testing:

## 1. Ownership Mindset

Did you proactively improve system after release?

## 2. Engineering Maturity

Can you identify bottlenecks scientifically?

## 3. Business Thinking

Did you reduce:

* runtime
* cost
* failures
* operational pain

## 4. Measurement Discipline

Can you quantify impact?

Strong engineers say:

* Job reduced from 90 min to 25 min
* Compute cost reduced 35%
* Failures reduced 80%

---

# Golden Structure to Answer

Always split into:

1. Performance optimization
2. Cost optimization
3. Reliability optimization
4. Developer productivity optimization
5. Governance / scalability optimization

This sounds senior.

---

# Real Project Context

Suppose pipeline:

Source Systems:

* POS transactions
* ERP master data
* CRM customer data

Processing:

Amazon S3 Raw Zone
AWS Glue / Databricks Spark
Gold marts
Amazon Redshift / BI

Daily SLA:

Reports ready by 7 AM.

---

# PART 1 — PERFORMANCE OPTIMISATIONS

---

# A. Partition-Aware MERGE Optimization

---

# Initial Problem

Gold fact table had 3 years of data.

Every day new sales data arrived.

Pipeline ran:

MERGE today data into Gold table.

But runtime became:

45 minutes.

---

# Why It Became Slow (Internals)

Even though only today’s rows changed, merge had to inspect many target files.

Spark internally does:

1. Read target table metadata
2. Identify candidate files
3. Match join keys
4. Rewrite affected files

If partition condition absent:

Many unnecessary files scanned.

As table grows:

* more files
* more metadata
* more IO
* more shuffle

So runtime worsens every month.

---

# Example

Gold table:

2023 data
2024 data
2025 data
2026 data

Today only:

2026-04-28

changed.

But merge scans large historical sections.

Wasteful.

---

# Optimization Applied

Add partition predicate:

Only merge into sale_date = current_date partition.

Meaning:

Today’s incoming data touches only today’s partition.

---

# What Spark Does Now

Instead of scanning 3 years:

Reads only relevant partition files.

Huge reduction in:

* file listing
* bytes read
* shuffle rows
* task count

---

# Result

45 minutes → 8 minutes.

---

# Why Interviewers Love This

Shows you understand:

* partition pruning
* merge internals
* table growth effects

---

# Interview Answer

We observed merge degradation as the target table grew. I redesigned the merge to be partition-aware so only impacted date partitions were scanned and rewritten. That reduced runtime dramatically.

---

# B. Small File Compaction

---

# Initial Problem

Upstream jobs wrote many tiny files.

Example:

20 GB dataset written as 8,000 files.

Average file size tiny.

---

# Why This Is Bad Internally

Spark query on read must:

1. List files from storage
2. Open many file handles
3. Read metadata/footer from each file
4. Schedule thousands of tasks

Actual data reading may be fast, but file overhead huge.

---

# Symptoms

Analysts say:

Simple query slow.

Spark UI shows:

Long startup before real compute.

---

# Optimization Applied

Compaction job weekly.

Combine tiny files into fewer optimal files.

Target:

128 MB to 1 GB each.

For Delta:

OPTIMIZE

For Parquet:

Repartition and rewrite.

---

# Result

Query latency reduced 60%.

---

# Why ZORDER Helps

If users filter by:

store_id, product_id

ZORDER co-locates similar values physically.

Less data scanned.

---

# Example

Without ZORDER:

store_id rows scattered everywhere.

With ZORDER:

store_id rows clustered.

---

# C. Parallel Ingestion

---

# Initial Problem

Workflow ran sequentially:

POS → ERP → CRM

Times:

POS 40 min
ERP 50 min
CRM 35 min

Total:

125 min

---

# Optimization Applied

Sources independent.

Run in parallel DAG branches.

Now runtime approx:

max(40,50,35) = 50 min + downstream time

---

# Result

125 min → ~60 min.

---

# Why It Works

Sequential adds durations.

Parallel bounded by slowest branch.

---

# D. Join Optimization

---

# Initial Problem

Large fact table joined with small dimension table using shuffle join.

Expensive shuffle.

---

# Optimization Applied

Broadcast small dimension.

---

# Internally

Spark sends small dimension to executors.

Each executor joins locally.

Avoids huge shuffle of fact table.

---

# Result

Join stage reduced significantly.

---

# E. AQE (Adaptive Query Execution)

Enabled AQE.

Spark adjusts at runtime:

* skew joins
* coalesce partitions
* join strategy switch

Very useful when data varies daily.

---

# PART 2 — COST OPTIMISATIONS

---

# A. Job Clusters vs Always-On Cluster

---

# Initial Problem

Cluster running 24/7.

Jobs need only 5 hours/day.

19 idle hours wasted.

---

# Optimization Applied

Ephemeral job cluster:

* starts at run time
* terminates after completion

---

# Result

Massive idle cost reduction.

---

# B. Spot / Preemptible Workers

---

# Why Good

Worker nodes can be interruptible.

Cheaper.

Use for executor nodes.

Keep driver stable on on-demand.

---

# Why Driver on Stable Node?

Driver manages:

* DAG scheduling
* job orchestration
* task tracking

If driver dies, whole run often fails.

---

# C. Incremental Processing

---

# Initial Problem

Daily full load of growing tables.

As history grows:

cost grows forever.

---

# Optimization Applied

CDC / watermark.

Read only changed rows.

---

# Example

10 TB total history.

Daily changes = 20 GB.

Read 20 GB only.

---

# Result

Huge recurring savings.

---

# D. Storage Lifecycle Policies

Raw files older than X days moved to cheaper tier.

Reduces storage spend.

---

# PART 3 — RELIABILITY OPTIMISATIONS

---

# A. Idempotent Writes

---

# Initial Problem

Job failed after partial insert.

Rerun appended duplicates.

---

# Optimization Applied

Use MERGE on business keys.

Same rerun = same final result.

---

# Why Important

Supports:

* retries
* backfills
* manual reruns

without corruption.

---

# B. Retry with Exponential Backoff

Transient failures:

* JDBC timeout
* network hiccup
* API 429

Instead of immediate fail:

Retry 1 min, then 2 min, then 5 min.

---

# C. Checkpoint / Control Tables

Track:

* batch id
* watermark
* rows read
* rows written
* status

If failure occurs:

Resume intelligently.

---

# D. Data Quality Gates

Before publish to Gold:

* null checks
* duplicate checks
* count reconciliation
* threshold anomalies

Prevents “green job, wrong data”.

---

# PART 4 — DEVELOPER EXPERIENCE OPTIMISATIONS

---

# A. Structured Audit Logging

---

# Initial Problem

Failures hidden in raw logs.

Hard to search.

---

# Optimization Applied

Write structured rows:

pipeline_name
run_id
step_name
status
error_code
error_message
start_time
end_time
records_processed

---

# Benefit

Simple query shows latest failures.

MTTR reduced sharply.

---

# B. Better Alerting

Instead of generic “job failed”.

Alert contains:

* pipeline name
* step failed
* run id
* likely cause

---

# C. Reusable Framework

Created config-driven ingestion framework.

New sources onboarded faster.

Less copy-paste code.

---

# PART 5 — SCALABILITY OPTIMISATIONS

---

# A. Dynamic Autoscaling

Cluster grows during peak loads.

Shrinks later.

---

# B. Better Partition Strategy

Partition by business date.

Not random columns.

Supports pruning.

---

# C. Avoid Overpartitioning

Too many tiny tasks = scheduler overhead.

Balanced partition count based on cores/data size.

---

# How To Discover What To Optimize (Very Senior Answer)

I did not guess optimizations.

I used evidence:

* Spark UI stage metrics
* long-running stages
* spill metrics
* file counts
* storage scans
* cluster utilization
* cost reports
* incident history

Then optimized biggest bottlenecks first.

That is powerful answer.

---

# Interview Ready Master Answer

Optimization was continuous in the project. I focused on performance, cost, reliability, and operability.

For performance, I reduced large merge runtimes by making merges partition-aware so only impacted date partitions were scanned. I also solved small file problems through compaction and improved query speed using clustering techniques. Independent ingestion pipelines were parallelized, reducing total SLA runtime.

For cost, I moved workloads to ephemeral job clusters, used spot workers where safe, and prioritized incremental loads instead of full scans.

For reliability, I implemented idempotent merge writes, retries for transient failures, and control tables for restartability.

For developer productivity, I added structured audit logging and operational dashboards, reducing mean time to diagnose incidents.

All improvements were metric-driven and measured after implementation.

---

# Trap Questions + Strong Answers

---

# Which optimization gave biggest ROI?

Incremental processing and partition-aware merges because they reduced recurring daily compute and runtime.

---

# Why not just increase cluster size?

Scaling hardware treats symptoms, not root cause. Logical optimizations often provide larger and cheaper gains.

---

# How did you validate improvement?

Compared before/after:

* runtime
* scanned bytes
* shuffle volume
* cluster hours
* query latency

---

# One-Line Memory Trick

Optimization = faster execution, lower cost, safer reruns, easier support.

---

# Senior-Level Closing Statement

I treat optimization as an engineering lifecycle, not a one-time tuning exercise.

---

# If you'd like, I can next give you **Top 30 PySpark optimizations with internals**, **Spark optimization from Catalyst + AQE perspective**, or **real project stories for every optimization**.
