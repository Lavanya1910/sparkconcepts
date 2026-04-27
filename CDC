# What Are Strategies Available for CDC Loads?

## First Understand What CDC Means

CDC means **Change Data Capture**.

Meaning:

Instead of loading the full table every time, load only rows that changed since last run.

Changed means:

* New rows inserted
* Existing rows updated
* Rows deleted

So instead of reading 10 crore rows daily, maybe read only 5,000 changed rows.

That saves:

* Time
* Cost
* Compute
* Network load

---

# Why Interviewer Asks This

They want to know:

* Do you understand incremental loading?
* Can you reduce full loads?
* Can you handle updates/deletes?
* Can you design scalable pipelines?

---

# Example Without CDC

Customer table has 10 million rows.

Every day if you reload full table:

* Slow
* Expensive
* Wasteful

Only 2,000 rows changed.

Better to load only changed rows.

That is CDC.

---

# Main CDC Strategies

---

# Strategy 1: Watermark / High Watermark

## What It Means

Track last processed timestamp or ID.

Example columns:

* updated_at
* modified_date
* transaction_id
* sequence_number

Store last successful value in control table.

Example:

Last loaded timestamp = 2026-04-27 10:00 AM

Next run query:

Select rows where updated_at greater than 10:00 AM

---

## Example in AWS Glue

Source = Microsoft or Oracle Corporation

Glue job runs hourly.

Reads:

Rows where updated_at > last watermark

Loads to Amazon S3 or Amazon Redshift

---

## Advantage

* Simple
* Easy to build
* Fast
* Common in batch ETL

## Limitation

If source updates row with old timestamp, you may miss it.

Example:

Today row changed, but updated_at mistakenly set to yesterday.

Then watermark logic misses it.

---

## Mitigation

Use overlap window.

Example:

Instead of 10:00 AM, read from 9:45 AM.

Then deduplicate downstream.

---

# Strategy 2: Log-Based CDC

## What It Means

Read database transaction logs directly.

Examples:

* Oracle redo logs
* SQL Server transaction log
* MySQL binlog

Tools:

Debezium
Apache Kafka

---

## How It Works

When source row changes:

Insert, update, delete event captured immediately.

Published to Kafka.

Then Spark / Glue Streaming consumes it.

---

## Example

Customer name changed.

Log emits:

Update customer_id 101 name Ravi to Ravanth

Pipeline applies update to target.

---

## Advantage

* Most accurate
* Near real-time
* Captures inserts, updates, deletes
* No full scans

## Limitation

* Complex setup
* DBA permissions needed
* Monitoring required

---

# Strategy 3: Full Load with Deduplication

## What It Means

Load full table each run, then merge changes.

Best for small tables.

Example:

store_master has 20,000 rows only.

Every night load full table.

Use merge on store_id.

---

## Advantage

* Very simple
* Good for small dimensions
* Easy maintenance

## Limitation

* Not good for large tables

---

# Handling Deletes

Very important interview topic.

---

## Soft Delete

Source row remains, but flag set:

is_deleted = true

Example:

customer active = false

Pipeline carries flag.

Gold layer filters deleted rows.

---

## Hard Delete

Source row physically removed.

Example:

Delete from customer where id = 101

Watermark method usually cannot detect this.

Why?

Deleted row no longer exists in source table.

Only log-based CDC can capture delete event reliably.

---

# In AWS Glue / EMR Terms

## Watermark CDC

Glue batch job every hour.

Reads rows where modified_date > last watermark.

Stores new watermark in DynamoDB / control table / RDS.

## Log CDC

Debezium captures Oracle changes → Kafka → Amazon EMR Spark Streaming → Amazon S3 / Redshift.

## Full Load Small Table

Glue nightly full extract + merge.

---

# How to Choose Strategy

## Use Watermark When:

* Batch system
* updated_at column exists
* moderate volume
* simple architecture needed

## Use Log CDC When:

* Real-time needed
* deletes important
* high transaction systems
* exact history required

## Use Full Load When:

* Small lookup tables
* Simplicity preferred

---

# Interview Ready Answer

We typically use three CDC strategies depending on source capability and business need.

First is watermark-based CDC, where we track the last processed updated timestamp or ID and fetch only newer records. This is common in batch Glue jobs and easy to implement.

Second is log-based CDC using tools like Debezium, where database transaction logs capture inserts, updates, and deletes in real time. We stream these events through Kafka and apply them downstream.

Third is full load with deduplication for small reference tables, where loading the full dataset daily is simpler than implementing CDC.

For deletes, soft deletes are propagated using flags, while hard deletes are best handled using log-based CDC.

---

# Strong Keywords for Senior Interviews

* High watermark
* Incremental load
* Log mining
* Idempotent merge
* Soft delete
* Hard delete
* Exactly once semantics
* Late arriving changes
* Overlap window

---

# Trap Question

## Why not always use watermark?

Good answer:

Watermark is simple but may miss hard deletes or backdated updates. For critical systems needing complete change history, log-based CDC is stronger.

---

# One-Line Memory Trick

Watermark = ask table what changed
Log CDC = listen to database changes live
Full load = reload all because table is small
