# Fantasy Football Data Warehouse & Analysis

An end-to-end streaming data pipeline that ingests, processes, and analyzes real-time fantasy football data from multiple sources.

## Architecture

This project implements a complete data pipeline with:

1. **Infrastructure Layer**: Docker containers for Kafka, PostgreSQL, Spark, MinIO, and Airflow
2. **Data Collection Layer**: API connectors for ESPN, NFL, Yahoo, Sleeper, and Pro Football Reference
3. **Data Processing Layer**: Spark jobs for transforming and analyzing data
4. **Storage Layer**: PostgreSQL data warehouse with a star schema and MinIO data lake
5. **Analytics Layer**: Streamlit dashboard for visualizing insights

## Key Components

- **Infrastructure Setup**: Docker services, database schema, Kafka topics, MinIO buckets
- **Data Collection**: API connectors and Kafka producers
- **Data Processing**: Spark jobs and Kafka consumers
- **Data Warehouse**: Star schema with dimension and fact tables
- **Analytics Dashboard**: Interactive Streamlit application

## Getting Started

1. Clone the repository
2. Run `chmod +x run_pipeline.sh`
3. Execute `./run_pipeline.sh start`
4. Access the dashboard at http://localhost:8501

## Technologies Used

- Apache Kafka
- Apache Spark
- PostgreSQL
- MinIO
- Apache Airflow
- Streamlit
- Python
- Docker