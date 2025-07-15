# Spark Stream Dynamic Workflow

# Getting started

## Kafka dev environment setup

A self-contained, Docker-based setup for a single-node Apache Kafka 3.9.0 broker, suitable for local development, testing, and CI/CD pipelines.

The Kafka container is configured to expose both internal and external listeners for seamless access from:
- other containers (via broker:29092)
- host machine (via localhost:9092)

The configuration is defined in the [`docker-compose.yml`](docker-compose.yml) file located in the root directory of the project.

To start the environment, simply run:

```bash
docker-compose up
```

#### Checking a Docker container is running:

```bash
docker ps
```

It should give you something like this:

```
CONTAINER ID   IMAGE                COMMAND                  CREATED         STATUS         PORTS                    NAMES
cf073c9deb9c   apache/kafka:3.9.0   "/__cacert_entrypoin…"   3 minutes ago   Up 3 minutes   0.0.0.0:9092->9092/tcp   broker
```

Look for `CONTAINER ID` and `NAMES`. The `CONTAINER ID` column is a hash that will be used later

Check instance health:
```bash
nc -zv localhost 9092
```

It everything is ok you'll get this:
```
Connection to localhost port 9092 [tcp/XmlIpcRegSvc] succeeded!
```

#### Exec into the broker container

```bash
docker exec -it broker bash
```

If it didn't work use this:
- `some Kafka images use sh`
   ```bash
   docker exec -it broker sh
    ```

Inside the container, test with:

```bash
kafka-topics.sh --bootstrap-server broker:29092 --list
```

If you get anything like his:
```
OCI runtime exec failed: exec failed: unable to start container process: exec: "cf073c9deb9c": executable file not found in $PATH: unknown
```

Use find the PATH:

```bash
find / -name kafka-topics.sh 2>/dev/null
```

This will search the full filesystem. On most Kafka images (especially ones based on bitnami, confluentinc, or apache/kafka), you should get something like this:

```
/opt/kafka/bin/kafka-topics.sh
```

Locate the actual path of kafka-topics.sh (inside the container we've already created):

Example fix:
```bash
/opt/kafka/bin/kafka-topics.sh --bootstrap-server broker:29092 --list
```

If you want to make *kafka-topics.sh* available globally:

```bash
export PATH=$PATH:/opt/kafka/bin
```

#### Create topics (Workflow streams):

1) Blogs stream topic

```bash
/opt/kafka/bin/kafka-topics.sh --bootstrap-server broker:29092 --create --topic Blogs --partitions 1 --replication-factor 1
```

Will log this:

```
Created topic Blogs.
```

2) Metrics stream topic

```bash
/opt/kafka/bin/kafka-topics.sh --bootstrap-server broker:29092 --create --topic Metrics --partitions 1 --replication-factor 1
```

Will log this:

```
Created topic Metrics.
```

3) Metrics stream topic

```bash
/opt/kafka/bin/kafka-topics.sh --bootstrap-server broker:29092 --create --topic Posts --partitions 1 --replication-factor 1
```

Will log this:

```
Created topic Posts.
```

Verify creation:

```bash
/opt/kafka/bin/kafka-topics.sh --bootstrap-server broker:29092 --list
```

Should return:
```
Blogs
Metrics
Posts
```

# Blogs Stream Job (Airflow + Spark)

This project is an automated pipeline for ingesting blog post data from a Kafka topic, transforming it using Apache Spark, and storing the results in HDFS. It is orchestrated using Apache Airflow and executed as a Spark job on YARN.

---

## Overview

The system consists of:

- An **Airflow DAG** (`blogs_stream_dynamic_worflow_job_dag`) that runs every hour.
- A **Scala Spark application** (`BlogsStreamApp`) which:
  - Reads blog data from a Kafka topic.
  - Optionally resumes from previously saved Kafka offsets.
  - Transforms and flattens the data.
  - Stores both data and Kafka offsets in HDFS.

---

## Airflow DAG: `blogs_stream_dynamic_worflow_job_dag`

### Schedule

- **Cron**: `0 * * * *` (hourly)
- **Catchup**: Disabled

### Components

- **`DummyOperator`**: Used for DAG start and end markers.
- **`SparkSubmitOperator`**: Launches the Scala Spark job on a YARN cluster.

### Parameters (Airflow Variable: `blogs_stream_dynamic_worflow_params`)

These are injected into the DAG from Airflow's `Variable.get()`:

| Key                   | Description                                  |
|-----------------------|----------------------------------------------|
| `kafkaHost`           | Kafka bootstrap server address               |
| `kafkaConsumerGroup`  | Kafka consumer group name                    |
| `blogsTopicName`      | Kafka topic containing blog data             |
| `hdfsPath`            | Output path in HDFS                          |
| `hdfsOffsetsPath`     | Path in HDFS to store Kafka offsets          |
| `excludes`            | JSON string with fields to exclude from data|
| `primary`             | Comma-separated list of primary fields       |

### Spark Configuration

- Dynamic allocation enabled (1–20 executors)
- Driver and executor memory: 8g
- Uses Kryo serialization
- Submits the job with:
  - Class: `org.cameron.cs.BlogsStreamApp`
  - JAR: `/usr/local/airflow/spark/blogs_stream_dynamic_worflow/blogs_stream_dynamic_worflow.jar`

---

## Spark Application: `BlogsStreamApp`

The entry point to the stream processing job. It uses:

- `BlogsStreamConfig`: Parsed CLI configuration
- `BlogsStreamProcessor`: Core logic for reading, transforming, and storing data

### Workflow Summary

1. **Read Kafka Offsets (if available)**:
  - Tries to resume from previously stored offsets in HDFS.
  - If unavailable, falls back to reading from the earliest Kafka messages.

2. **Read Kafka Stream**:
  - Kafka data is read as a DataFrame using `spark.read`.

3. **Flatten and Transform**:
  - Uses `from_json` and schema inference to parse message values.
  - Optionally excludes fields (`--excludes`) and keeps only primary fields (`--primary`).
  - Adds a current timestamp column.

4. **Write Output**:
  - Final data is written to a partitioned HDFS path using the current execution date and time.
  - Kafka offsets are saved alongside to allow fault-tolerant continuation.

---

## Notable Scala Classes

### `BlogsStreamProcessor`

Responsible for the following:

- **readBlogsStreamWithOffsets**:
  - Reads Kafka data starting from specific offsets
- **readBlogsStream**:
  - Reads Kafka data from the earliest offset
- **transformBlogs**:
  - Filters out unwanted fields and applies additional transformation
- **process**:
  - Coordinates the full ETL: Read → Transform → Save Data + Offsets

---

# Metrics Stream Job (Airflow + Spark)

This repository contains a pipeline that ingests metrics data from a Kafka topic, processes and transforms it using Apache Spark, and stores the results in HDFS. The pipeline is orchestrated with Apache Airflow and supports fault-tolerant Kafka offset handling for exactly-once semantics.

---

## Overview

The pipeline consists of:

- An **Airflow DAG** (`metrics_dynamic_workflow_stream_dag.py`) that runs every hour.
- A **Spark Streaming job** implemented in Scala (`MetricsStreamApp`) that:
  - Reads from a Kafka topic
  - Applies schema-based parsing and transformation
  - Writes both metrics data and Kafka offsets to HDFS

---

## Airflow DAG: 'metrics_stream_dynamic_workflow_dag'

### Schedule

- **Cron**: `0 * * * *` (hourly)
- **Catchup**: Disabled

### Operators Used

- `DummyOperator`: Marks start and end of DAG
- `SparkSubmitOperator`: Launches Spark job on a YARN cluster

### Configuration (via Airflow Variable: `metrics_stream_dynamic_workflow_params`)

| Key                   | Description                                  |
|-----------------------|----------------------------------------------|
| `kafkaHost`           | Kafka bootstrap server address               |
| `kafkaConsumerGroup`  | Kafka consumer group name                    |
| `metricsTopicName`    | Kafka topic with metrics data                |
| `hdfsPath`            | Path in HDFS to store metrics data           |
| `hdfsOffsetsPath`     | Path in HDFS to store Kafka offsets          |

### Spark Configuration

| Setting                                 | Value      |
|-----------------------------------------|------------|
| Dynamic allocation                      | Enabled    |
| Executors (min/max)                     | 1 / 20     |
| Driver memory                           | 8g         |
| Executor memory                         | 8g         |
| Executor cores                          | 2          |
| Serializer                              | Kryo       |
| Queue                                   | airflow    |

### Spark Application Parameters

Passed via `application_args`:

-d <execution_date>

-h <kafkaHost>

-g <kafkaConsumerGroup>

-t <metricsTopicName>

-p <hdfsPath>

-o <hdfsOffsetsPath>

---

## Spark Application: `MetricsStreamApp`

This is the main class that sets up the Spark session and invokes the core processing logic via `MetricsStreamProcessor`.

---

## MetricsStreamProcessor Logic

### Workflow Summary

1. **Offset Management**:
  - Attempts to resume processing using offsets saved in HDFS.
  - Falls back to reading from `earliest` if offsets are missing or unreadable.

2. **Kafka Stream Read**:
  - Reads raw data from Kafka using the inferred JSON schema.
  - Adds metadata fields: `CurrentTimestamp`, `KafkaPartition`, `KafkaOffset`.

3. **Transformation**:
  - Extracts and renames relevant fields from the nested JSON structure.
  - Adds a hashed ID from `metricsurl`.
  - Includes the current `execDate` for partitioning.

4. **Data Write**:
  - Outputs transformed data to:
    ```
    <hdfsPath>/<YYYYMMDD>/<HH-mm>
    ```
  - Saves Kafka offsets to:
    ```
    <hdfsOffsetsPath>
    ```
---

# Posts Stream Job (Airflow + Spark)

This project defines an **Apache Airflow DAG** and a **Spark application** for processing social media post data from Kafka and storing the structured, partitioned output in **HDFS**. It supports configurable field exclusions, primary key selection, and Kafka offset checkpointing.

---

### Spark Configuration

- Dynamic allocation enabled (1–20 executors)
- Driver and executor memory: 8g
- Uses Kryo serialization
- Submits the job with:
  - Class: `org.cameron.cs.PostsStreamApp`
  - JAR: `/usr/local/airflow/spark/blogs_stream_dynamic_worflow/posts_stream.jar`

---

## Spark Application: `PostsStreamApp`

The entry point to the stream processing job. It uses:

- `PostsStreamConfig`: Parsed CLI configuration
- `PostsStreamProcessor`: Core logic for reading, transforming, and storing data


## DAG Overview

- **DAG ID**: `posts_stream_dynamic_workflow_job_dag`
- **Schedule**: Every 3 hours
- **Start Date**: 2025-03-13
- **Retries**: 1 (with 5-minute delay)
- **Catchup**: Disabled
- **Input**: Kafka topic (posts)
- **Output**: HDFS Parquet files

---

## Configuration via Airflow Variable

All configuration is passed through an Airflow Variable named `posts_stream_dynamic_workflow_params`, in JSON format.

### Required Parameters

| Key                   | Description                                           |
|-----------------------|-------------------------------------------------------|
| `kafkaHost`           | Kafka bootstrap servers (e.g., `kafka:9092`)          |
| `kafkaConsumerGroup`  | Kafka consumer group ID                               |
| `postsTopicName`      | Kafka topic containing post messages                  |
| `hdfsPath`            | Base HDFS output path                                 |
| `hdfsOffsetsPath`     | HDFS path for storing Kafka offsets (for checkpoints) |
| `excludes`            | JSON object of fields to exclude in output            |
| `primary`             | Comma-separated string of "primary" fields to keep    |

---
### Operators

- **DummyOperator**: Marks start and end of DAG execution.
- **SparkSubmitOperator**: Runs a Spark job on YARN with dynamic allocation and custom configuration.

## Kafka Streaming Behaviour

- The pipeline tries to **resume from the last stored offset** (found in HDFS).
- If no offset is found (e.g., first run or failure), it defaults to reading from the **earliest Kafka offset**.
- Offsets are saved again at the end of each run to maintain data consistency.

---

## Schema Detection

- The schema of the incoming Kafka messages is **automatically inferred** from the data payload.
- This allows dynamic adaptation to changes in structure (e.g., new fields or nested structures).

---

## Data Transformation Logic

### Processed Components

The pipeline breaks the data into the following logical components:

| Output Name          | Description                                                |
|----------------------|------------------------------------------------------------|
| **Primary**          | User-defined subset of key fields kept as-is              |
| **Other**            | Flattened and cleaned post metadata (excludes applied)     |
| **Attachments**      | Structured and exploded attachment data                    |
| **GeoData**          | Location metadata extracted from posts                     |
| **ModificationInfo** | Historical edit/versioning metadata (if present)           |

Nested exclusions are prefixed (e.g., AuthorAvatarUrl). Top-level exclusions are dropped directly.

Output Structure in HDFS
Each processed component is written to a separate HDFS path, partitioned by execution date and time (formatted as YYYYMMDD/HH-mm):

