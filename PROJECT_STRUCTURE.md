# S3-Kafka-Flink-Pipeline Project Structure

## Overview
This project implements a real-time streaming data pipeline that orchestrates data flow from AWS S3 → Kafka → Flink → PostgreSQL → Grafana using Apache Airflow for orchestration.

## Directory Structure

```
s3-kafka-flink-pipeline/
│
├── 📁 dags/                          # Airflow DAGs (Directed Acyclic Graphs)
│   ├── taxi_pipeline_dag.py         # Main orchestration DAG
│   └── orch.py                      # (Empty/backup file)
│
├── 📁 producer/                      # S3 to Kafka Producer Service
│   ├── dockerfile                    # Docker image definition for producer
│   ├── requirements.txt              # Python dependencies (boto3, kafka-python, pandas, pyarrow)
│   ├── s3_to_kafka.py               # Main producer script (reads S3, sends to Kafka)
│   └── venv/                        # Python virtual environment (local development)
│
├── 📁 jobs/                          # Flink Processing Jobs
│   └── kafka_consumer_ui.py         # Flink PyFlink job (consumes Kafka, transforms, loads to Postgres)
│
├── 📁 lib/                           # Flink Connector Libraries (JAR files)
│   ├── flink-connector-jdbc-3.1.2-1.17.jar      # JDBC connector for PostgreSQL
│   ├── flink-connector-kafka-1.17.2.jar        # Kafka connector
│   ├── flink-sql-connector-kafka-1.17.2.jar    # Kafka SQL connector
│   ├── kafka_2.12-3.5.1.jar                    # Kafka client library
│   ├── kafka-clients-3.5.1.jar                 # Kafka clients
│   ├── postgresql-42.6.0.jar                   # PostgreSQL JDBC driver
│   └── lib/                          # Nested lib directory (duplicates)
│
├── 📁 logs/                          # Airflow Execution Logs
│   ├── dag_id=taxi_streaming_pipeline/  # Logs organized by DAG and run ID
│   └── scheduler/                    # Airflow scheduler logs
│
├── 📁 connect/                       # (Appears empty - possibly for Kafka Connect)
│
├── 📁 flink-job/                     # (Appears empty - possibly for Flink job artifacts)
│
├── 📄 docker-compose.yml             # Docker Compose configuration
│   └── Defines all services:
│       - zookeeper (Kafka dependency)
│       - kafka (message broker)
│       - postgres (data warehouse)
│       - flink-jobmanager (Flink cluster manager)
│       - flink-taskmanager (Flink worker)
│       - s3-producer (data producer container)
│       - grafana (visualization)
│       - airflow-webserver (Airflow UI)
│       - airflow-scheduler (Airflow scheduler)
│
├── 📄 Dockerfile-airflow             # Custom Airflow image with Docker client
│
├── 📄 fix_docker_socket.sh          # Helper script to fix Docker socket mounting
│
└── 📄 *.jar                          # Root-level JAR files (duplicates of lib/)
    ├── flink-connector-jdbc-3.1.2-1.17.jar
    ├── flink-connector-kafka-1.17.2.jar
    ├── flink-sql-connector-kafka-1.17.2.jar
    ├── kafka-clients-3.6.0.jar
    └── postgresql-42.6.0.jar
```

## Component Details

### 1. **dags/** - Airflow Orchestration
- **Purpose**: Contains Airflow DAG definitions that orchestrate the entire pipeline
- **Files**:
  - `taxi_pipeline_dag.py`: Main DAG with tasks:
    - Service readiness checks (Kafka, Flink, Postgres)
    - S3 to Kafka producer execution
    - Flink job submission
    - Data verification

### 2. **producer/** - Data Producer Service
- **Purpose**: Reads data from AWS S3 and publishes to Kafka
- **Components**:
  - `s3_to_kafka.py`: 
    - Connects to AWS S3 using boto3
    - Reads Parquet files
    - Converts to JSON and streams to Kafka topic `s3-taxi-trips`
  - `dockerfile`: Builds Python 3.12 image with dependencies
  - `requirements.txt`: boto3, kafka-python, pandas, pyarrow, numpy

### 3. **jobs/** - Flink Processing Jobs
- **Purpose**: Contains Flink streaming jobs for data transformation
- **Files**:
  - `kafka_consumer_ui.py`: PyFlink job that:
    - Consumes from Kafka topic `s3-taxi-trips`
    - Cleans and validates data
    - Enriches with calculated fields (trip duration, speed, fare metrics)
    - Loads transformed data into PostgreSQL `taxi_trips_dashboard` table

### 4. **lib/** - Flink Connector Libraries
- **Purpose**: JAR files required by Flink for connectors
- **Libraries**:
  - Kafka connectors (for consuming from Kafka)
  - JDBC connector (for writing to PostgreSQL)
  - PostgreSQL driver (database connectivity)
  - Kafka client libraries

### 5. **logs/** - Airflow Execution Logs
- **Purpose**: Stores execution logs for each DAG run
- **Structure**: Organized by DAG ID, run ID, and task ID
- **Usage**: Debugging and monitoring pipeline execution

### 6. **docker-compose.yml** - Infrastructure Definition
- **Purpose**: Defines all services and their configurations
- **Services**:
  - **Zookeeper**: Required by Kafka for coordination
  - **Kafka**: Message broker (port 9092)
  - **PostgreSQL**: Database (port 5432, database: taxi_db)
  - **Flink JobManager**: Flink cluster coordinator (port 8081)
  - **Flink TaskManager**: Flink worker nodes
  - **s3-producer**: Producer container (built from ./producer)
  - **Grafana**: Visualization dashboard (port 3000)
  - **Airflow**: Web UI (port 8085) and scheduler

## Data Flow

```
AWS S3 (Parquet files)
    ↓
[Producer Container] - s3_to_kafka.py
    ↓
Kafka Topic: s3-taxi-trips
    ↓
[Flink Job] - kafka_consumer_ui.py
    ↓ (transformation & enrichment)
PostgreSQL Table: taxi_trips_dashboard
    ↓
Grafana Dashboard (visualization)
```

## Key Configuration Files

1. **docker-compose.yml**: 
   - Service definitions
   - Network configuration
   - Volume mounts (dags, jobs, lib)
   - Environment variables

2. **Dockerfile-airflow**:
   - Custom Airflow image
   - Includes Docker client for container management
   - Python dependencies (psycopg2)

3. **producer/dockerfile**:
   - Python 3.12 base image
   - Installs producer dependencies
   - Sets CMD to run s3_to_kafka.py

## Volume Mounts (in docker-compose.yml)

- `./dags` → `/opt/airflow/dags` (Airflow DAGs)
- `./jobs` → `/opt/flink/jobs` (Flink job files)
- `./lib` → `/opt/flink/usrlib` (Flink connector libraries)
- `/var/run/docker.sock` → `/var/run/docker.sock` (Docker socket for Airflow)

## Network Architecture

All containers run on the same Docker network (created by docker-compose), allowing them to communicate using service names:
- `kafka:9092` - Kafka broker
- `postgres:5432` - PostgreSQL database
- `flink-jobmanager:8081` - Flink JobManager
- `flink-taskmanager` - Flink TaskManager

## Port Mappings

- **8085** → Airflow Web UI
- **8081** → Flink Web UI
- **3000** → Grafana
- **9092** → Kafka
- **5432** → PostgreSQL
- **2181** → Zookeeper

## Execution Flow

1. **Airflow DAG triggers** → `taxi_pipeline_dag.py`
2. **Service checks** → Verify Kafka, Flink, Postgres are ready
3. **Producer task** → Starts s3-producer container, reads S3, sends to Kafka
4. **Flink job task** → Submits Flink job to consume, transform, and load data
5. **Verification task** → Checks data in PostgreSQL
6. **Grafana** → Visualizes the data from PostgreSQL

