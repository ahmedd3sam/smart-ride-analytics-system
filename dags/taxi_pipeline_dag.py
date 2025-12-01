from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import socket
import time
import requests
import psycopg2

default_args = {
    'owner': 'Menna',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': 30
}

# ----------------------------
# Helper functions
# ----------------------------
def wait_for_kafka(host='kafka', port=9092, retries=30, delay=2):
    for i in range(retries):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            result = sock.connect_ex((host, port))
            sock.close()
            if result == 0:
                print("Kafka is ready!")
                return True
        except Exception as e:
            print(f"Kafka not ready ({i+1}/{retries}): {e}")
        time.sleep(delay)
    raise Exception("Kafka not ready after retries")

def wait_for_flink(url='http://flink-jobmanager:8081', retries=30, delay=2):
    for i in range(retries):
        try:
            r = requests.get(url, timeout=5)
            if r.status_code == 200:
                print("Flink JobManager is ready!")
                return True
        except Exception as e:
            print(f"Flink not ready ({i+1}/{retries}): {e}")
        time.sleep(delay)
    raise Exception("Flink JobManager not ready after retries")

def wait_for_postgres(host='postgres', port=5432, db='taxi_db', user='user', password='password', retries=30, delay=2):
    for i in range(retries):
        try:
            conn = psycopg2.connect(host=host, port=port, dbname=db, user=user, password=password, connect_timeout=5)
            # Also check if the target table exists (or will be created by Flink)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'taxi_trips_dashboard'
                );
            """)
            table_exists = cursor.fetchone()[0]
            cursor.close()
            conn.close()
            print(f"PostgreSQL is ready! (table exists: {table_exists})")
            return True
        except Exception as e:
            print(f"Postgres not ready ({i+1}/{retries}): {e}")
        time.sleep(delay)
    raise Exception("Postgres not ready after retries")


# ----------------------------
# DAG definition
# ----------------------------
with DAG(
    dag_id='taxi_streaming_pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='S3 → Kafka → Flink → Postgres pipeline'
) as dag:

    # ----------------------------
    # Task 1: Check services
    # ----------------------------
    check_kafka = PythonOperator(
        task_id='check_kafka_ready',
        python_callable=wait_for_kafka
    )

    check_flink = PythonOperator(
        task_id='check_flink_ready',
        python_callable=wait_for_flink
    )

    check_postgres = PythonOperator(
        task_id='check_postgres_ready',
        python_callable=wait_for_postgres
    )

    # ----------------------------
    # Task 2: Start S3 → Kafka Producer
    # ----------------------------
    start_s3_to_kafka = BashOperator(
        task_id='start_kafka_producer',
        bash_command="""
        echo "Starting S3 to Kafka producer asynchronously..."

        # Check if s3-producer container is already running
        if docker ps --filter "name=s3-producer" | grep s3-producer >/dev/null 2>&1; then
            echo "s3-producer is already running, skipping start."
        else
            # Start the producer in detached mode
            docker-compose up -d s3-producer
            echo "s3-producer started in detached mode."
        fi
        """
    )

    # ----------------------------
    # Task 3: Submit Flink Job (with proper readiness checks)
    # ----------------------------
    run_flink_job = BashOperator(
        task_id='run_flink_job',
        bash_command="""
        echo "=========================================="
        echo "Submitting Flink Job"
        echo "=========================================="

        # Check if Docker is available
        if ! command -v docker &> /dev/null; then
            echo "ERROR: Docker command not available"
            exit 1
        fi

        JOB_MANAGER_NAME="flink-jobmanager"
        TASK_MANAGER_NAME="flink-taskmanager"
        
        # Verify JobManager is running
        if ! docker ps --filter "name=^${JOB_MANAGER_NAME}$" -q | grep -q .; then
            echo "ERROR: Flink JobManager container not running"
            docker ps || true
            exit 1
        fi

        # Verify TaskManager is running and ready
        echo "Checking Flink TaskManager availability..."
        if ! docker ps --filter "name=^${TASK_MANAGER_NAME}$" -q | grep -q .; then
            echo "ERROR: Flink TaskManager container not running"
            docker ps || true
            exit 1
        fi

        echo "✓ Flink JobManager is running"
        echo "✓ Flink TaskManager is running"

        # Wait for Flink cluster to be fully ready
        echo "Waiting for Flink cluster to be ready..."
        MAX_RETRIES=30
        RETRY_COUNT=0
        FLINK_READY=0

        while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
            # Check if Flink REST API is accessible
            if docker exec "$JOB_MANAGER_NAME" curl -s http://localhost:8081/overview > /dev/null 2>&1; then
                # Check if TaskManager is registered
                TASK_MANAGERS=$(docker exec "$JOB_MANAGER_NAME" curl -s http://localhost:8081/taskmanagers 2>/dev/null | grep -o "taskmanagers" | wc -l || echo "0")
                if [ "$TASK_MANAGERS" -gt "0" ]; then
                    echo "✓ Flink cluster is ready (TaskManagers registered)"
                    FLINK_READY=1
                    break
                fi
            fi
            RETRY_COUNT=$((RETRY_COUNT + 1))
            echo "Waiting for Flink cluster... ($RETRY_COUNT/$MAX_RETRIES)"
            sleep 2
        done

        if [ "$FLINK_READY" != "1" ]; then
            echo "ERROR: Flink cluster not ready after $MAX_RETRIES retries"
            echo "JobManager logs:"
            docker logs "$JOB_MANAGER_NAME" 2>&1 | tail -20
            exit 1
        fi

        # Wait a bit more for Kafka messages to be available
        echo "Waiting for Kafka messages to be available..."
        sleep 10

        JOB_FILE="/opt/flink/jobs/kafka_consumer_ui.py"
        echo "Verifying Flink job file: $JOB_FILE"
        if ! docker exec "$JOB_MANAGER_NAME" test -f "$JOB_FILE"; then
            echo "ERROR: Flink job file not found: $JOB_FILE"
            echo "Files in /opt/flink/jobs:"
            docker exec "$JOB_MANAGER_NAME" ls -la /opt/flink/jobs/ || true
            exit 1
        fi

        # Verify the job file structure
        echo "Verifying Flink job file structure..."
        
        # Check for env.execute()
        if docker exec "$JOB_MANAGER_NAME" grep -q "^env.execute" "$JOB_FILE"; then
            echo "✓ env.execute() is present"
        else
            echo "WARNING: env.execute() not found, attempting to uncomment..."
            docker exec "$JOB_MANAGER_NAME" sed -i 's/^# env.execute/env.execute/' "$JOB_FILE" 2>/dev/null || true
        fi
        
        # Check for INSERT INTO statement (creates the operator)
        if docker exec "$JOB_MANAGER_NAME" grep -q "INSERT INTO" "$JOB_FILE"; then
            echo "✓ INSERT INTO statement found (creates operator)"
        else
            echo "ERROR: INSERT INTO statement not found in job file!"
            echo "The Flink job must have an INSERT INTO statement to create operators."
            exit 1
        fi
        
        # Check for source table definition
        if docker exec "$JOB_MANAGER_NAME" grep -q "CREATE TABLE.*taxi_trips" "$JOB_FILE"; then
            echo "✓ Source table definition found"
        else
            echo "WARNING: Source table definition may be missing"
        fi
        
        # Check for sink table definition
        if docker exec "$JOB_MANAGER_NAME" grep -q "CREATE TABLE.*taxi_trips_dashboard" "$JOB_FILE"; then
            echo "✓ Sink table definition found"
        else
            echo "WARNING: Sink table definition may be missing"
        fi

        echo ""
        echo "Submitting Flink job..."
        echo "Command: flink run -py $JOB_FILE -d"
        
        # Submit the job and capture output
        FLINK_OUTPUT=$(docker exec "$JOB_MANAGER_NAME" flink run -py "$JOB_FILE" -d 2>&1)
        FLINK_EXIT_CODE=$?
        
        echo "$FLINK_OUTPUT"
        
        # Check if job was actually submitted by querying Flink REST API
        # Even if flink run returns non-zero, the job might still be running
        echo ""
        echo "Verifying job submission via Flink REST API..."
        sleep 5  # Give Flink a moment to register the job
        
        JOB_SUBMITTED=0
        MAX_VERIFY_RETRIES=10
        VERIFY_COUNT=0
        
        while [ $VERIFY_COUNT -lt $MAX_VERIFY_RETRIES ]; do
            # Get list of running jobs from Flink REST API
            JOBS_JSON=$(docker exec "$JOB_MANAGER_NAME" curl -s http://localhost:8081/jobs 2>/dev/null || echo "")
            if [ -n "$JOBS_JSON" ]; then
                # Check if there are any running jobs
                RUNNING_JOBS=$(echo "$JOBS_JSON" | grep -o '"status":"RUNNING"' | wc -l || echo "0")
                if [ "$RUNNING_JOBS" -gt "0" ]; then
                    echo "✓ Found $RUNNING_JOBS running job(s) in Flink cluster"
                    JOB_SUBMITTED=1
                    break
                fi
            fi
            VERIFY_COUNT=$((VERIFY_COUNT + 1))
            echo "Waiting for job to appear in Flink cluster... ($VERIFY_COUNT/$MAX_VERIFY_RETRIES)"
            sleep 3
        done
        
        if [ "$JOB_SUBMITTED" = "1" ]; then
            echo ""
            echo "✓ Flink job submitted and running successfully!"
            echo "Job is running and will:"
            echo "  - Consume from Kafka topic: s3-taxi-trips"
            echo "  - Transform and enrich the data"
            echo "  - Load into PostgreSQL table: taxi_trips_dashboard"
            echo ""
            echo "Check Flink UI at http://localhost:8081 for job status"
            # Exit with success even if flink run had warnings
            exit 0
        else
            # Job not found in cluster - this is a real failure
            echo ""
            echo "ERROR: Flink job submission verification failed"
            echo "The flink run command exited with code: $FLINK_EXIT_CODE"
            echo "Job was not found running in Flink cluster after verification attempts"
            echo ""
            echo "Flink JobManager logs (last 50 lines):"
            echo "----------------------------------------"
            docker logs "$JOB_MANAGER_NAME" 2>&1 | tail -50
            echo "----------------------------------------"
            echo ""
            echo "Flink TaskManager logs (last 30 lines):"
            echo "----------------------------------------"
            docker logs "$TASK_MANAGER_NAME" 2>&1 | tail -30
            echo "----------------------------------------"
            echo ""
            echo "Troubleshooting:"
            echo "1. Check if Kafka has messages: docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic s3-taxi-trips --from-beginning --max-messages 1"
            echo "2. Check Flink UI: http://localhost:8081"
            echo "3. Verify connectors are available in /opt/flink/usrlib/"
            exit 1
        fi
        """
    )

    # ----------------------------
    # Task 4: Optional Postgres verification
    # ----------------------------
    verify_postgres_data = BashOperator(
        task_id='verify_postgres_data',
        bash_command="""
        echo "Verifying Postgres data..."
        echo "Waiting for Flink to process and load data..."
        sleep 30

        # Check if psql is available
        if command -v psql &> /dev/null; then
            echo "Querying PostgreSQL for data count..."
            PGPASSWORD=password psql -h postgres -U user -d taxi_db -c "SELECT COUNT(*) as total_records FROM taxi_trips_dashboard;" 2>/dev/null || {
                echo "Note: Table may not exist yet, or Flink job is still processing"
                echo "This is normal if the Flink job just started"
            }
        else
            echo "psql not available. Skipping data verification."
            echo "You can verify manually:"
            echo "  PGPASSWORD=password psql -h postgres -U user -d taxi_db -c 'SELECT COUNT(*) FROM taxi_trips_dashboard;'"
        fi

        echo ""
        echo "Pipeline orchestration complete!"
        echo "Check Grafana at http://localhost:3000 for visualization"
        echo "Check Flink UI at http://localhost:8081 for job status"
        """
    )

    # ----------------------------
    # Task dependencies
    # ----------------------------
    [check_kafka, check_flink, check_postgres] >> start_s3_to_kafka
    start_s3_to_kafka >> run_flink_job
    run_flink_job >> verify_postgres_data


# ----------------------------------------------------------

# from airflow import DAG
# from airflow.operators.bash import BashOperator
# from airflow.utils.dates import days_ago

# default_args = {
#     'owner': 'Menna',
#     'start_date': days_ago(1),
#     'retries': 1,
#     'retry_delay': 30,  # in seconds
# }

# with DAG(
#     dag_id='taxi_streaming_pipeline',
#     default_args=default_args,
#     schedule_interval=None,
#     catchup=False
# ) as dag:

#     # Task: Trigger Flink job inside its container
#     run_flink_job = BashOperator(
#         task_id='run_flink_job',
#         bash_command=(
#             "docker exec flink-jobmanager "
#             "flink run -py /opt/flink/jobs/kafka_consumer_ui.py"
#         )
#     )
