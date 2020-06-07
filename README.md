# Data Engineering Nanodegree

Projects developed for [Data Engineering Nanodegree](https://www.udacity.com/course/data-engineer-nanodegree--nd027) offered by Udacity.

### Project 1: [Data Modeling with PostgreSQL](https://github.com/AlexVagionas/Data-Engineering-Nanodegree/tree/master/1-Data-Modeling-with-Postgres)

Modeling of user activity data from a music streaming app in a relational database. The database is designed to optimize queries for understanding what songs users are listening to.
* Created a relational database using PostgreSQL
* Developed an ETL pipeline to convert the data into a star schema.

Utilized: Python, Pandas, SQL, PostgreSQL, Star Schema

### Project 2: [Data Modeling with Apache Cassandra](https://github.com/AlexVagionas/Data-Engineering-Nanodegree/tree/master/2-Data-Modeling-with-Apache-Cassandra)
Modeling of user activity data from a music streaming app in a NoSQL Apache Cassandra database. The database is designed to run specific queries for understanding what songs users are listening to.
* Created a NoSQL database using Apache Cassandra
* Developed denormalized tables, optimized for the requested queries

Utilized: Python, CQL, Apache Cassandra, Denormalization

### Project 3: [Cloud Data Warehouse](https://github.com/AlexVagionas/Data-Engineering-Nanodegree/tree/master/3-Cloud-Data-Warehouse)
Migration of data warehouse for music streaming app to the cloud using Amazon Web Services.

* Created Redshift Cluster and IAM roles.
* Developed an ETL pipeline to extract data from S3 bucket, and load the processed tables into Redshift

Utilized: Python, SQL, AWS S3, AWS Redshift, Infrastructure as code (IaC)

### Project 4: [Spark Data Lake](https://github.com/AlexVagionas/Data-Engineering-Nanodegree/tree/master/4-Spark-Data-Lake)
Creation of ETL pipeline for a Data Lake using Spark and Amazon Web Services
* Created of multi-node Hadoop Clusters using AWS EMR
* Developed ETL pipeline for copying data from S3 and processing them using Spark SQL and DataFrames
* Stored processed data in S3 bucket as .parquet files

Utilized: Python, PySpark, AWS S3, AWS EMR, Parquet

### Project 5: [Data Pipelines with Airflow](https://github.com/AlexVagionas/Data-Engineering-Nanodegree/tree/master/5-Data-Pipelines-with-Airflow)
Automation of ETL pipeline for data warehouse using Apache Airflow
* Scheduled end-to-end data pipeline to run on an hourly basis using Apache Airflow
* Created custom airflow operators for each task
* Created custom subDAG for batch execution of common tasks
* Wrote data quality checks to ensure validity of data in database

Utilized: Python, Apache Airflow, AWS S3, AWS Redshift

### Capstone Project: [US Tourism & Immigration Data Model](https://github.com/AlexVagionas/Data-Engineering-Nanodegree/tree/master/6-Capstone-Project)
Creation of ETL pipeline to tranform and compine data on cities' demographics, cities' average temperature and neighbouring airports with U.S tourism and immigration dataset, containing millions of rows, in order to gain insight into the causes of popularity of certain migration ports.

Utilized: Python, Pandas, PySpark, AWS CLI, AWS S3, AWS EMR, Parquet
