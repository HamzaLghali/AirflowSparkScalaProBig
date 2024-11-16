# AirflowSparkScalaProBig
# AirflowSparkScalaProBig

## Overview
Welcome to the **AirflowSparkScalaProBig** project! This repository demonstrates a comprehensive **Big Data Engineering** solution leveraging:

- **PostgreSQL** for structured data storage
- **Google Cloud Platform (GCP)** for scalable cloud infrastructure
- **Apache Kafka** for real-time data streaming
- **Apache Airflow** for orchestration
- **Databricks** for advanced data analytics and collaborative data engineering
- **Scala** implementing the **Factory Design Pattern** for flexible and maintainable data workflows
- **Medallion Architecture** for data processing (Bronze, Silver, Gold layers)
- **Slowly Changing Dimensions (SCD)** for managing historical data

This solution is designed to efficiently process, transform, and manage large-scale datasets while maintaining high data quality and scalability.

## Features
- **ETL/ELT Pipelines:** Implement robust data workflows using Airflow, Databricks, and Spark.
- **Real-Time Streaming:** Integrate Kafka for ingesting and processing live data streams.
- **Factory Design Pattern:** Ensure code modularity and reusability in data processing.
- **Medallion Architecture:** Organize data into Bronze, Silver, and Gold layers for clarity and purpose-driven processing.
- **Slowly Changing Dimensions (SCD):** Manage historical data changes efficiently.
- **Cloud Integration:** Utilize GCP and Databricks services for storage, computation, and scalability.

## Architecture Diagram
Include an architecture diagram here if available. 

---

## Getting Started

### Prerequisites
Ensure you have the following installed:

- **Docker** (for running Kafka and Airflow locally)
- **Apache Airflow**
- **Apache Kafka**
- **Scala** (version 2.12.x)
- **Apache Spark** (version 3.3.0)
- **Databricks CLI**
- **PostgreSQL**
- **Google Cloud SDK** (for interacting with GCP)

### Installation

1. Clone this repository:
   ```bash
   git clone https://github.com/HamzaLghali/AirflowSparkScalaProBig.git
   cd AirflowSparkScalaProBig
   ```

2. Set up Docker containers for Kafka:
   ```bash
   docker-compose up -d
   ```

3. Install Python dependencies for Airflow:
   ```bash
   pip install apache-airflow
   ```

4. Set up PostgreSQL:
   - Configure your database connection details in the Airflow `connections` section.

5. Configure Databricks:
   - Install the Databricks CLI:
     ```bash
     pip install databricks-cli
     ```
   - Authenticate the CLI:
     ```bash
     databricks configure --token
     ```
   - Upload the compiled JAR file to your Databricks workspace.

6. Compile the Scala project:
   ```bash
   sbt clean assembly
   ```

7. Run Airflow:
   ```bash
   airflow standalone
   ```

8. Start the Kafka producer and consumer from your Scala project to handle real-time data streams.

9. Run the DAGs in Airflow to trigger the ETL workflows, including Databricks jobs.

---

## Repository Structure
```
AirflowSparkScalaProBig
├── airflow
│   ├── dags
│   │   └── big_data_pipeline.py    # Airflow DAG definitions
│   └── scripts
│       └── runspark.sh            # Shell script to trigger Spark jobs
├── kafka
│   ├── producer.scala             # Kafka producer implementation
│   └── consumer.scala             # Kafka consumer implementation
├── scala
│   ├── src
│   │   └── main
│   │       └── scala
│   │           └── com
│   │               └── Factory    # Factory Design Pattern classes
│   └── resources
│       └── application.conf       # Configuration files
├── docker-compose.yml             # Docker setup for Kafka and Airflow
└── README.md                      # Project documentation
```

---

## Data Workflow
### Medallion Architecture
1. **Bronze Layer**
   - Raw data ingestion from Kafka topics and PostgreSQL tables.
2. **Silver Layer**
   - Transformation and cleaning using Apache Spark and Databricks.
3. **Gold Layer**
   - Aggregated and refined datasets for analytics and reporting.

### Slowly Changing Dimensions (SCD)
Utilize PostgreSQL, Spark, and Databricks to handle type-1 and type-2 SCD scenarios, maintaining both current and historical data states.

---

## Key Components

- **Airflow DAGs:** Orchestrate workflows with dependency management.
- **Databricks Jobs:** Advanced analytics and collaborative data engineering.
- **Spark Jobs:** Perform data transformations using Scala and Spark.
- **Kafka Producers/Consumers:** Handle real-time streaming data.
- **Medallion Architecture:** Bronze, Silver, Gold layers for data organization.
- **SCD Management:** Maintain historical and current data states effectively.

---

## Contributing
Contributions are welcome! Feel free to submit issues or pull requests to improve this project.

---

## Contact
Developed by **Hamza Lghali**. 
- [GitHub](https://github.com/HamzaLghali)
- [LinkedIn](https://linkedin.com/in/hamza-lghali)

Feel free to reach out for collaborations or inquiries!

