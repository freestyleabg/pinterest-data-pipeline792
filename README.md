# AWS Data Pipeline Project: Emulating Pinterest's Data Handling

This document outlines the implementation of a data pipeline that mirrors Pinterest's approach to managing large volumes of data. The pipeline integrates various AWS services with Databricks, Spark, Airflow, Kinesis, Kafka, and API Gateway for both batch and real-time data processing.

## Architecture Diagram

![Processing Pipeline](images/CloudPinterestPipeline.png)

## Project Workflow

### Data Emulation

The `user_posting_emulation.py` script was developed to simulate user data postings. This script is designed to:
- Interact with databases for data retrieval.
- Serialize datetime objects for compatibility.
- Post data concurrently to Kafka and Kinesis streams through configured endpoints.

### Data Ingestion

The developed scripts automate the process of posting simulated data to both Kafka and Kinesis. The functionality includes:
- Managing database connections and data retrieval.
- Formatting and sending data to different endpoints configured for each service.

### Databricks Notebooks

Databricks notebooks are utilized for tasks such as data processing:

- **Batch Data Processing:** `batch_processing_pipeline.ipynb` facilitates batch data processing using Spark SQL and Databricks.
- **Stream Data Processing:** `stream_processing_pipeline.ipynb` is dedicated to processing streaming data from AWS Kinesis, involving schema definition, data cleaning, and transformation.
- **Pipeline Utilities:** `pipeline_utils.ipynb` includes utilities for managing interactions with AWS services, such as loading AWS credentials, mounting and managing S3 buckets. It also provides core functions for data transformation and processing tasks essential for both batch and stream processing pipelines.


### Data Cleaning and Transformation

Key steps in the data cleaning process include:
- Addressing missing values and erroneous entries.
- Converting data types for accuracy and consistency.
- Renaming columns and restructuring data for enhanced usability and analysis.

### Schema Definition and Stream Processing

For effective stream processing, specific schemas are defined for Pinterest posts, geolocation data, and user profiles. This structured approach ensures streamlined data manipulation and storage in Delta tables, supporting real-time analytics.

### Automation with Airflow

The project employs AWS Managed Workflows for Apache Airflow (MWAA) to automate Databricks notebook executions, ensuring data is processed regularly and efficiently.

## Detailed Architecture and Tools

- **Apache Kafka and AWS MSK:** Form the backbone of the data streaming infrastructure, enabling robust data ingestion and storage mechanisms.
- **Databricks and Apache Spark:** Provide the computational power for processing both batch and streaming data, facilitating rapid distributed computing and analysis.
- **AWS Kinesis:** Enhances real-time data processing capabilities, integrating seamlessly with other AWS services.
- **AWS S3:** Acts as the primary storage solution, offering durability and accessibility for processed data.
- **Amazon API Gateway:** Used for configuring endpoints for both batch and real-time data ingestion. The API Gateway facilitates communication with Kafka through the `/{proxy+}` endpoint and with Kinesis through structured endpoints under `/streams`.


## Project Structure

**Databricks Notebooks:**

- `batch_processing_pipeline.ipynb` - Handles batch data processing.
- `stream_processing_pipeline.ipynb` - Manages real-time data stream processing.
- `database_utils.ipynb` - Provides utilities for database management.

**User Posting Scripts:**

- `user_posting_emulation.py` - Simulates user data postings to Kafka and Kinesis.

**Airflow DAG:**

- `Oa5040edb649_dag.py` - Defines the workflow for automating the execution of Databricks notebooks.

**Miscellaneous:**

- `.gitignore`, `Oa5040edb649-key-pair.pem`, `config.json`, `credentials` - Includes configuration files, keys for secure connections, and AWS access details.


