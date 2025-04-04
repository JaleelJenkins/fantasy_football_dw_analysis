#!/bin/bash

# Color definitions
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Print a colored message
print_message() {
  echo -e "${2}${1}${NC}"
}

# Check if Docker is running
docker_running() {
  if ! docker info > /dev/null 2>&1; then
    print_message "Docker is not running. Please start Docker first." "${RED}"
    exit 1
  fi
}

# Check Docker Compose version
check_compose() {
  if docker compose version > /dev/null 2>&1; then
    COMPOSE_CMD="docker compose"
  elif docker-compose --version > /dev/null 2>&1; then
    COMPOSE_CMD="docker-compose"
  else
    print_message "Docker Compose not found. Please install Docker Compose." "${RED}"
    exit 1
  fi
}

# Check if containers are running
check_containers_running() {
  if [ "$($COMPOSE_CMD ps -q | wc -l)" -gt "0" ]; then
    return 0
  else
    return 1
  fi
}

# Function to start the infrastructure
start_infrastructure() {
  print_message "Starting infrastructure (Kafka, PostgreSQL, Airflow, Spark, MinIO)..." "${BLUE}"
  $COMPOSE_CMD up -d kafka kafka-ui postgres airflow-webserver spark-master spark-worker minio
  
  # Wait for services to be ready
  print_message "Waiting for services to be ready..." "${YELLOW}"
  sleep 10
  
  # Check if services are running
  if ! $COMPOSE_CMD ps | grep -q "kafka.*running"; then
    print_message "Kafka failed to start. Check logs with: $COMPOSE_CMD logs kafka" "${RED}"
    exit 1
  fi
  
  if ! $COMPOSE_CMD ps | grep -q "postgres.*running"; then
    print_message "PostgreSQL failed to start. Check logs with: $COMPOSE_CMD logs postgres" "${RED}"
    exit 1
  fi
  
  print_message "Infrastructure services started successfully!" "${GREEN}"
}

# Function to initialize the database
init_database() {
  print_message "Initializing database..." "${BLUE}"
  
  # Run database setup script
  $COMPOSE_CMD run --rm -v $(pwd):/app -w /app jupyter python db_setup.py
  
  if [ $? -eq 0 ]; then
    print_message "Database initialized successfully!" "${GREEN}"
  else
    print_message "Database initialization failed. Check the logs above." "${RED}"
    exit 1
  fi
}

# Function to set up Kafka topics
setup_kafka() {
  print_message "Setting up Kafka topics..." "${BLUE}"
  
  # Run Kafka setup script
  $COMPOSE_CMD run --rm -v $(pwd):/app -w /app jupyter python kafka_setup.py
  
  if [ $? -eq 0 ]; then
    print_message "Kafka topics created successfully!" "${GREEN}"
  else
    print_message "Kafka setup failed. Check the logs above." "${RED}"
    exit 1
  fi
}

# Function to setup MinIO buckets
setup_minio() {
  print_message "Setting up MinIO buckets..." "${BLUE}"
  
  # Run MinIO setup script
  $COMPOSE_CMD run --rm -v $(pwd):/app -w /app jupyter python minio_setup.py
  
  if [ $? -eq 0 ]; then
    print_message "MinIO buckets created successfully!" "${GREEN}"
  else
    print_message "MinIO setup failed. Check the logs above." "${RED}"
    exit 1
  fi
}

# Function to start data collection
start_data_collection() {
  print_message "Starting Kafka producer for data collection..." "${BLUE}"
  
  # Run the Kafka producer in detached mode
  $COMPOSE_CMD run -d --name fantasy_producer -v $(pwd):/app -w /app jupyter python streaming/kafka_producer.py
  
  print_message "Kafka producer started successfully!" "${GREEN}"
}

# Function to start data processing
start_data_processing() {
  print_message "Starting Kafka consumer for data processing..." "${BLUE}"
  
  # Run the Kafka consumer in detached mode
  $COMPOSE_CMD run -d --name fantasy_consumer -v $(pwd):/app -w /app jupyter python streaming/kafka_consumer.py
  
  print_message "Kafka consumer started successfully!" "${GREEN}"
}

# Function to start the dashboard
start_dashboard() {
  print_message "Starting Streamlit dashboard..." "${BLUE}"
  
  # Run the Streamlit dashboard
  $COMPOSE_CMD run -d --name fantasy_dashboard -p 8501:8501 -v $(pwd):/app -w /app jupyter streamlit run dashboard/fantasy_dashboard.py
  
  print_message "Dashboard started successfully!" "${GREEN}"
  print_message "Access the dashboard at http://localhost:8501" "${YELLOW}"
}

# Function to display service URLs
show_service_urls() {
  print_message "\nService URLs:" "${BLUE}"
  print_message "Dashboard:       http://localhost:8501" "${GREEN}"
  print_message "Kafka UI:        http://localhost:8080" "${GREEN}"
  print_message "Airflow:         http://localhost:8081" "${GREEN}"
  print_message "Spark Master UI: http://localhost:9090" "${GREEN}"
  print_message "MinIO UI:        http://localhost:9001" "${GREEN}"
  print_message "Jupyter Lab:     http://localhost:8888 (token required)" "${GREEN}"
}

# Function to stop all services
stop_services() {
  print_message "Stopping all services..." "${BLUE}"
  
  # Stop specific containers
  $COMPOSE_CMD stop fantasy_producer fantasy_consumer fantasy_dashboard
  
  # Remove specific containers
  $COMPOSE_CMD rm -f fantasy_producer fantasy_consumer fantasy_dashboard
  
  # Stop all infrastructure services
  $COMPOSE_CMD down
  
  print_message "All services stopped successfully!" "${GREEN}"
}

# Main function
main() {
  docker_running
  check_compose
  
  case "$1" in
    "start")
      start_infrastructure
      init_database
      setup_kafka
      setup_minio
      start_data_collection
      start_data_processing
      start_dashboard
      show_service_urls
      ;;
    "stop")
      stop_services
      ;;
    "restart")
      stop_services
      sleep 5
      main start
      ;;
    "dashboard")
      start_dashboard
      ;;
    "producer")
      start_data_collection
      ;;
    "consumer")
      start_data_processing
      ;;
    "status")
      $COMPOSE_CMD ps
      show_service_urls
      ;;
    *)
      print_message "Usage: $0 {start|stop|restart|dashboard|producer|consumer|status}" "${YELLOW}"
      exit 1
      ;;
  esac
}

# Execute main function with command line arguments
main "$@"