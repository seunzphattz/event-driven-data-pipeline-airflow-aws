This project implements a cloud-native, distributed batch data pipeline using AWS services and Apache Airflow for orchestration.

It demonstrates a production-style architecture where:

* **Apache Airflow** orchestrates workflow execution
* **AWS Glue (Spark)** performs distributed ETL processing
* **Amazon S3** acts as the data lake storage layer
* **AWS Glue Python job** loads processed metrics
* **Amazon DynamoDB** serves as the low-latency metrics store

The solution models a real-world batch analytics workflow and reflects best practices in distributed data engineering.

---

## Architecture Overview

```
Raw Data (S3)
        ↓
Airflow DAG (Conditional Orchestration)
        ↓
Glue Spark Job (Distributed ETL)
        ↓
Processed Output (S3)
        ↓
Glue Python Loader
        ↓
DynamoDB (Metrics Table)
```

---

## Architectural Principles Applied

### 1. Separation of Concerns

* Orchestration logic isolated in Airflow
* Distributed compute handled by Glue Spark
* Storage managed via S3
* Serving layer implemented with DynamoDB

### 2. Distributed Processing

The Spark job leverages Glue’s managed distributed runtime to:

* Perform large-scale transformations
* Compute aggregations and KPIs
* Handle parallelized processing across executors

### 3. Conditional Workflow Execution

The Airflow DAG:

* Validates required input datasets
* Branches execution using `BranchPythonOperator`
* Enforces dependency ordering
* Waits for Glue job completion before progressing

### 4. Idempotent Data Loading

The DynamoDB loader uses upsert logic to:

* Prevent duplicate metric insertion
* Support repeated pipeline runs safely

### 5. Storage Lifecycle Awareness

Processed files are archived post-execution to:

* Avoid reprocessing
* Maintain pipeline hygiene
* Simulate production file lifecycle management

---

## Project Structure

```
.
├── dag_script/
│   └── dag_glue_workflow.py
│
├── glue_scripts/
│   ├── glue_pyspark.py
│   └── glue_dynamo.py
│
├── local_dev/
│   └── local_docker_development.sh
│
├── users_sample.csv
├── songs_sample.csv
├── stream_sample.csv
└── README.md
```

---

## Component Breakdown

### Airflow DAG (Orchestration Layer)

Responsibilities:

* Validate presence of required S3 input prefixes
* Trigger Glue Spark ETL job
* Poll for job completion
* Trigger downstream loader job
* Archive processed files
* Skip execution if prerequisites are not met

This reflects enterprise-grade orchestration patterns commonly deployed in MWAA environments.

---

### Glue Spark ETL (Compute Layer)

Responsibilities:

* Read raw datasets from S3
* Apply transformations and aggregations
* Generate streaming KPIs and metrics
* Write structured output back to S3

Demonstrates:

* Distributed execution
* Aggregation logic
* Spark-based transformation pipeline
* Cloud-native ETL design

---

### Glue Python Job (Serving Layer Loader)

Responsibilities:

* Read processed output
* Insert or update DynamoDB records
* Maintain metric consistency

This separates analytical compute from serving-layer updates — a common production pattern.

---

## Data Engineering Patterns Demonstrated

* Distributed batch processing
* Cloud-native orchestration
* Managed Spark infrastructure
* Decoupled compute and storage
* Branch-based DAG control flow
* Explicit job dependency management
* Programmatic Glue job triggering
* S3 prefix validation strategy
* Upsert-based DynamoDB writes

---

## Local Development Strategy

Pyspark is run locally using Docker to simulate orchestration behavior before deploying to AWS.

This allows:

* Safe script testing
* Workflow debugging
* Faster development iteration

---

## Production Considerations

In an enterprise deployment, this pipeline would include:

* Glue Job Bookmarking for incremental processing
* Partitioned S3 layout for performance optimization
* IAM least-privilege role policies
* CloudWatch monitoring and alerting
* Event-driven triggering via EventBridge
* Infrastructure-as-Code provisioning (CloudFormation/Terraform)
* Cost optimization via DPU tuning
* CI/CD for DAG deployment

---

## Why This Project Matters

This repository reflects:

* Practical understanding of distributed data systems
* Real-world orchestration patterns
* AWS-native data engineering practices
* Architectural thinking beyond simple scripts

It demonstrates how modern batch data pipelines are structured in production environments using managed cloud services.

## Cost
Less than $2
