# Data Pipelines with Airflow

## Description

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

In this project, Apache Airflow is utilized to pipeline the data using custom operators for staging, processing and peforming quality checks. The data pipeline is scheduled to run every hour and also process past data with backfills.

## Files and Folders in Project

`dags` Folder containing the main pipeline DAG and a subDAG to load all 4 dimension tables
`plugins` Folder with custom Apache Airflow operators scripts
`redshift` Folder containing python scripts for Redshift database
