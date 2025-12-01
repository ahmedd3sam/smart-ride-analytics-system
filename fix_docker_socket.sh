#!/bin/bash
# Script to recreate Airflow containers with Docker socket mount

echo "Stopping Airflow containers..."
docker-compose stop airflow-scheduler airflow-webserver

echo "Removing Airflow containers..."
docker-compose rm -f airflow-scheduler airflow-webserver

echo "Recreating Airflow containers with Docker socket mount..."
docker-compose up -d airflow-webserver airflow-scheduler

echo "Waiting for containers to start..."
sleep 5

echo "Verifying Docker socket mount..."
if docker exec airflow-scheduler ls -la /var/run/docker.sock > /dev/null 2>&1; then
    echo "✓ Docker socket is mounted successfully!"
    echo "Testing Docker access..."
    docker exec airflow-scheduler docker ps > /dev/null 2>&1
    if [ $? -eq 0 ]; then
        echo "✓ Docker is accessible from Airflow containers!"
    else
        echo "✗ Docker socket mounted but not accessible (permission issue?)"
    fi
else
    echo "✗ Docker socket is NOT mounted. Check docker-compose.yml"
fi

echo ""
echo "Done! You can now run the DAG again."

