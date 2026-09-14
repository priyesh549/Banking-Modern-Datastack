# 🏦 Banking Modern Data Stack

![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue?logo=postgresql)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-7.4-black?logo=apachekafka)
![Debezium](https://img.shields.io/badge/Debezium-2.2-red?logo=debezium)
![MinIO](https://img.shields.io/badge/MinIO-S3%20Storage-red?logo=minio)
![Snowflake](https://img.shields.io/badge/Snowflake-Cloud%20DWH-29B5E8?logo=snowflake)
![dbt](https://img.shields.io/badge/dbt-Transformations-FF694B?logo=dbt)
![Airflow](https://img.shields.io/badge/Airflow-Orchestration-017CEE?logo=apacheairflow)
![Docker](https://img.shields.io/badge/Docker-Containerized-2496ED?logo=docker)
![GitHub Actions](https://img.shields.io/badge/GitHub%20Actions-CI%2FCD-black?logo=githubactions)

An end-to-end **modern data engineering pipeline for a banking use case**, built around PostgreSQL CDC, Kafka, MinIO, Snowflake, dbt, Airflow, Docker, and GitHub Actions.

The project simulates banking data, captures database changes in near real time, lands CDC events as Parquet files in object storage, loads them into Snowflake, transforms them with dbt, maintains historical dimensions using SCD Type 2 snapshots, and validates deployments through CI/CD.

---

## 🏗️ Architecture

```text
                  ┌──────────────────────┐
                  │   Faker Generator    │
                  │ Customers / Accounts │
                  │    Transactions      │
                  └──────────┬───────────┘
                             │
                             ▼
                  ┌──────────────────────┐
                  │     PostgreSQL       │
                  │      OLTP Source     │
                  └──────────┬───────────┘
                             │
                       WAL / CDC
                             │
                             ▼
                  ┌──────────────────────┐
                  │      Debezium        │
                  │   Kafka Connect      │
                  └──────────┬───────────┘
                             │
                             ▼
                  ┌──────────────────────┐
                  │        Kafka         │
                  │  CDC Event Streams   │
                  └──────────┬───────────┘
                             │
                             ▼
                  ┌──────────────────────┐
                  │   Python Consumer    │
                  │      Batch →         │
                  │      Parquet         │
                  └──────────┬───────────┘
                             │
                             ▼
                  ┌──────────────────────┐
                  │       MinIO          │
                  │    Raw Data Lake     │
                  └──────────┬───────────┘
                             │
                     Airflow DAG
                             │
                             ▼
                  ┌──────────────────────┐
                  │      Snowflake       │
                  │     RAW Layer       │
                  └──────────┬───────────┘
                             │
                           dbt
                             │
                             ▼
                  ┌──────────────────────┐
                  │    Staging Models    │
                  └──────────┬───────────┘
                             │
                             ▼
                  ┌──────────────────────┐
                  │   Facts / Dimensions │
                  │     + SCD Type 2     │
                  └──────────────────────┘

        Git Push ──► GitHub Actions CI ──► Main ──► CD ──► Snowflake
```

---

## 🔄 Data Flow

### 1. Banking Data Generation

Python + Faker generates synthetic:

* Customers
* Accounts
* Transactions

The data is inserted into PostgreSQL, which acts as the transactional OLTP source.

### 2. Change Data Capture

**Debezium** monitors PostgreSQL WAL changes and publishes CDC events to Kafka topics:

```text
banking_server.public.customers
banking_server.public.accounts
banking_server.public.transactions
```

### 3. Kafka → MinIO

A Python Kafka consumer reads CDC events and batches records before writing them as **Parquet files** into MinIO.

Example:

```text
s3://raw/accounts/date=YYYY-MM-DD/accounts_<timestamp>.parquet
```

### 4. MinIO → Snowflake

An **Airflow DAG** periodically:

1. Reads Parquet files from MinIO
2. Downloads them
3. Uploads them to Snowflake internal table stages
4. Executes `COPY INTO` to load the RAW tables

### 5. dbt Transformation

dbt builds the analytical layer:

```text
RAW
 │
 ▼
Staging Views
 │
 ├── stg_customers
 ├── stg_accounts
 └── stg_transactions
       │
       ▼
Analytics Models
 ├── dim_customers
 ├── dim_accounts
 └── fact_transactions
```

The staging models also deduplicate customer/account records by selecting the latest record per ID.

### 6. SCD Type 2

dbt snapshots maintain historical versions of:

* Customers
* Accounts

Tracked attributes include changes such as:

```text
Customer:
first_name
last_name
email

Account:
customer_id
account_type
balance
```

The resulting dimensions expose:

* `effective_from`
* `effective_to`
* `is_current`

### 7. CI/CD

GitHub Actions provides automated validation and deployment.

**CI — `ci.yml`**

Runs on pushes to `main` / `dev` and pull requests to `main`:

* Python dependency installation
* Ruff linting
* Pytest execution
* dbt dependency installation
* dbt compilation

**CD — `cd.yml`**

Runs after changes are pushed to `main`:

* Installs dbt Snowflake adapter
* Creates the production dbt profile from GitHub Secrets
* Runs `dbt deps`
* Runs `dbt run`
* Runs `dbt test`

Both CI and CD have been successfully validated for this project.

---

## 🛠️ Tech Stack

| Technology     | Purpose                                       |
| -------------- | --------------------------------------------- |
| Python         | Data generation and Kafka → MinIO consumer    |
| PostgreSQL     | Banking OLTP source                           |
| Debezium       | Change Data Capture                           |
| Apache Kafka   | CDC event streaming                           |
| MinIO          | S3-compatible raw object storage              |
| Snowflake      | Cloud data warehouse                          |
| dbt            | SQL transformations, models and SCD snapshots |
| Apache Airflow | Data ingestion and snapshot orchestration     |
| Docker Compose | Local infrastructure                          |
| GitHub Actions | CI/CD                                         |
| Ruff           | Python linting                                |
| Pytest         | Test execution                                |

---

## 📂 Project Structure

```text
banking-modern-datastack/
│
├── .github/
│   └── workflows/
│       ├── ci.yml
│       └── cd.yml
│
├── banking_dbt/
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_customers.sql
│   │   │   ├── stg_accounts.sql
│   │   │   └── stg_transactions.sql
│   │   ├── marts/
│   │   │   ├── dimensions/
│   │   │   └── facts/
│   │   └── sources.yml
│   ├── snapshots/
│   │   ├── customers_snapshot.sql
│   │   └── accounts_snapshot.sql
│   └── dbt_project.yml
│
├── consumer/
│   └── kafka_to_minio.py
│
├── data-generator/
│   └── faker_generator.py
│
├── kafka-debezium/
│   └── generator_and_post_connector.py
│
├── postgres/
│   └── schema.sql
│
├── docker/
│   └── dags/
│       ├── minio_to_snowflake_dag.py
│       └── scd_snapshots.py
│
├── docker-compose.yml
├── dockerfile-airflow.dockerfile
├── requirments.txt
├── ruff.toml
└── README.md
```

---

## 🚀 Running the Project

### Prerequisites

* Docker Desktop
* Python 3.11+
* Snowflake account
* Git

### 1. Configure environment variables

Create/update the required `.env` files with PostgreSQL, Kafka, MinIO and Snowflake configuration.

**Do not commit credentials or secrets to Git.**

### 2. Start the infrastructure

```bash
docker compose up -d
```

This starts the core services including:

* PostgreSQL
* Kafka
* Zookeeper
* Debezium Kafka Connect
* MinIO
* Airflow

### 3. Initialize the PostgreSQL schema

Apply:

```text
postgres/schema.sql
```

### 4. Start the data generator

```bash
python data-generator/faker_generator.py
```

For a single generation cycle:

```bash
python data-generator/faker_generator.py --once
```

### 5. Register the Debezium connector

```bash
python kafka-debezium/generator_and_post_connector.py
```

### 6. Start the Kafka consumer

```bash
python consumer/kafka_to_minio.py
```

The consumer batches Kafka CDC records and writes them to MinIO as Parquet.

### 7. Run dbt

From the dbt project:

```bash
cd banking_dbt

dbt deps
dbt run
dbt test
```

For SCD Type 2 snapshots:

```bash
dbt snapshot
```

---

## 🔐 CI/CD Configuration

GitHub Actions uses repository secrets for Snowflake and PostgreSQL credentials.

Required Snowflake secrets include:

```text
SNOWFLAKE_ACCOUNT
SNOWFLAKE_USER
SNOWFLAKE_PASSWORD
SNOWFLAKE_WAREHOUSE
```

The workflows create the dbt profile dynamically during execution, so credentials are not stored in the repository.

---

## 🎯 Key Engineering Concepts Demonstrated

* Change Data Capture (CDC)
* PostgreSQL WAL-based replication
* Event streaming with Kafka
* Debezium Kafka Connect
* Batch processing of streaming events
* Parquet-based data lake storage
* S3-compatible object storage
* Cloud data warehousing with Snowflake
* dbt staging and analytical models
* Incremental dbt models
* Slowly Changing Dimensions — Type 2
* Airflow DAG orchestration
* Dockerized data infrastructure
* Python data engineering
* Automated linting and validation
* Git-based CI/CD

---

## 📌 Project Outcome

This project demonstrates how transactional banking data can move from an OLTP database through a **CDC-driven streaming pipeline** into a cloud analytics platform while preserving historical changes and applying automated data transformations and validation.

The complete development workflow is:

```text
PostgreSQL
    ↓
Debezium
    ↓
Kafka
    ↓
Python Consumer
    ↓
MinIO / Parquet
    ↓
Airflow
    ↓
Snowflake
    ↓
dbt
    ↓
Analytics + SCD Type 2
    ↓
GitHub Actions CI/CD
```

---

## 👨‍💻 Skills Demonstrated

**Data Engineering:** Python, SQL, ETL/ELT, CDC, streaming, data modeling

**Data Platform:** PostgreSQL, Kafka, Debezium, MinIO, Snowflake

**Transformation & Orchestration:** dbt, Airflow

**DevOps:** Docker, Git, GitHub Actions, CI/CD

**Data Modeling:** Fact tables, dimensions, incremental models, SCD Type 2
