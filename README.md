# STEDI Human Balance Analytics

## Overview

This project leverages AWS Glue, AWS S3, Athena, IAM roles, VPC gateway, Python, Spark, SQL, and etc to build a lakehouse solution for the STEDI Step Trainer. The solution processes and curates sensor data to support machine learning models that accurately detect steps.

## Key Requirements

1. **Simulate Data Sources:**

   - Create S3 directories for `customer_landing`, `step_trainer_landing`, and `accelerometer_landing` zones.
   - Populate these directories with initial data.

2. **Create Glue Tables:**

   - Create Glue tables for `customer_landing` and `accelerometer_landing`.
   - Query these tables using Athena and take screenshots of the results.

3. **Sanitize and Curate Data:**

   - Create Glue jobs to sanitize customer and accelerometer data, creating `customer_trusted` and `accelerometer_trusted` tables.
   - Address data quality issues and create a `customers_curated` table.

4. **Aggregation for Machine Learning:**
   - Create a `step_trainer_trusted` table with data from customers who agreed to share their data.
   - Create a `machine_learning_curated` table that aggregates step trainer and accelerometer readings.

## Conclusion

This project sets up a data pipeline to process and curate sensor data, enabling accurate real-time step detection through machine learning models.
