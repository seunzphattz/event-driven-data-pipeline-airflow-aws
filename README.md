# Event-Driven Data Pipeline on AWS (Airflow, S3, Glue, DynamoDB)

Production-style AWS data pipeline demonstrating orchestration, distributed ETL, and serving-layer design.

## Tech Highlights

Airflow • AWS Glue • S3 • DynamoDB • PySpark • Distributed ETL • Orchestration

---

## Overview

This project implements a cloud-native, distributed batch data pipeline using AWS services and Apache Airflow. It simulates a production-grade analytics system where data is ingested, processed at scale, and served for low-latency access.

---

## Architecture (High-Level)


```text
S3 (Raw Data)
    ↓
Airflow (Orchestration Layer)
    ↓
AWS Glue Spark (Distributed ETL)
    ↓
S3 (Processed Data)
    ↓
Glue Python Loader
    ↓
DynamoDB (Serving Layer)
```

**Flow:** Airflow → S3 → Glue → S3 → DynamoDB

---

## What This Pipeline Does

- Orchestrates workflows using Apache Airflow  
- Processes large-scale data using AWS Glue (Spark)  
- Stores raw and processed data in Amazon S3  
- Loads computed metrics into DynamoDB for fast access  
- Implements conditional execution and dependency management  

---

## Key Engineering Concepts Demonstrated

- Distributed data processing (Spark on Glue)  
- Workflow orchestration (Airflow DAGs)  
- Separation of compute, storage, and serving layers  
- Idempotent data loading (safe re-runs)  
- Conditional branching logic in pipelines  
- Cloud-native architecture design  

---

## Data Flow

1. Raw datasets are stored in S3  
2. Airflow DAG validates input availability  
3. Glue Spark job performs distributed ETL and computes metrics  
4. Processed data is written back to S3  
5. Glue Python job loads metrics into DynamoDB  
6. Processed files are archived to prevent reprocessing  

---

## Project Structure

```text
dag_script/
  dag_glue_workflow.py

glue_scripts/
  glue_pyspark.py
  glue_dynamo.py

local_dev/
  local_docker_development.sh

data/
  users_sample.csv
  songs_sample.csv
  stream_sample.csv
```

---

## Architectural Principles

### 1. Separation of Concerns
- Airflow handles orchestration  
- Glue Spark handles distributed compute  
- S3 handles storage  
- DynamoDB handles serving  

### 2. Distributed Processing
- Spark performs large-scale transformations  
- Parallel execution across executors  

### 3. Conditional Workflow Execution
- DAG validates input datasets  
- BranchPythonOperator controls execution paths  
- Ensures dependency ordering  

### 4. Idempotent Data Loading
- DynamoDB uses upsert logic  
- Safe for repeated runs without duplication  

### 5. Storage Lifecycle Management
- Processed files are archived  
- Prevents duplicate processing  
- Maintains clean pipeline state  

---

## System Design Thinking

This pipeline was designed with the following considerations:

- Scalability: Distributed Spark jobs handle large datasets  
- Reliability: Airflow manages retries and dependencies  
- Data durability: S3 provides persistent storage  
- Low-latency serving: DynamoDB enables fast access to metrics  
- Maintainability: Clear separation between orchestration, processing, and serving layers  

---

## Local Development

PySpark jobs are tested locally using Docker before deployment to AWS.

This allows:

- Faster iteration  
- Safer debugging  
- Environment consistency  

---

## Production Considerations

- Incremental processing (Glue bookmarks)  
- Partitioned S3 storage  
- IAM least-privilege access  
- Monitoring via CloudWatch  
- Event-driven triggers (EventBridge)  
- CI/CD for deployment  
- Cost optimisation (DPU tuning)  

---

## Why This Project Matters

This project demonstrates:

- Real-world data pipeline design  
- Distributed data engineering practices  
- AWS-native architecture patterns  
- System-level thinking beyond simple ETL scripts  

---

## Cost

Approximate cost: <$2 (development/testing scale)
