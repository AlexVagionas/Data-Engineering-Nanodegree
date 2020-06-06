# US Tourism & Immigration Data Model

## Project Summary

US National Tourism and Trade Office gathers detailed data on every person arriving to the US who is not a citizen or lawful permanent resident including their country of origin, age, purpose of travel, duration of stay, mode of transportation, port of arrival etc.

This project attempts to combine the above data with information about temperature and demographics in US cities and the existing airports in each US state. For this purpose, an ETL pipeline is created using Spark which runs on an Amazon EMR cluster. The data are loaded in Spark and transformed into fact and dimension DataFrames that form a star schema. These DataFrames are then stored as .parquet files in an Amazon S3 Bucket.

## Files and Folders in Project

`datasets`: Directory containing all datasets
`output`: Directory where processed data will be stored if S3 is not choosen 
`I94_SAS_Labels_Descriptions.SAS`: Label descriptions for immigration dataset
`Project Description.ipynb`: Project description and exploration of datasets

Jupyter Notebooks describing data pipeline for each dataset:
`us_immigration.ipynb`, `us_cities_demographics.ipynb`, `us_cities_temperature.ipynb`, `state_airports.ipynb`

Python scipts for data pipeline:
`etl.py`, `us_immigration_etl.py`, `us_cities_demographics_etl.py`, `us_cities_temperature_etl.py`, `state_airport_etl.py`, `sas_labels_processing.py`

`aws.cfg`: AWS configurations
`capstone_project.pem`: EC2 secret key used to connect to EMR cluster
`create_emr_cluster.bat`: Batch script for EMR Cluster creation

## Execution in AWS EMR

#### Prerequisites

To run the project using etl.py the `datasets` directory should be copied to an S3 bucket, an IAM role with read and write access to S3 should be created and the access/secret key pair should be inserted in `aws.cfg`:

    AWS_ACCESS_KEY_ID=<insert access key>
    AWS_SECRET_ACCESS_KEY=<insert secret access key>

The name of S3 bucket should also be inserted in `aws.cfg`:

    S3=s3a://<instert bucket name>/

Also an EC2 key should be created as described in:
https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-gs-prerequisites.html#emr-gs-key-pair

#### Cluster Initialization

Create the EMR cluster by running `create_emr_cluster.bat`. 

Copy `etl.py`, `us_immigration_etl.py`, `us_cities_demographics_etl.py`, `us_cities_temperature_etl.py`, `state_airport_etl.py`, `sas_labels_processing.py`, `I94_SAS_Labels_Descriptions.SAS` and `aws.cfg` files to the cluster by running in bash terminal:

```bash
scp -i ./capstone_project.pem <file name> hadoop@ec2-**-***-***-***.us-west-2.compute.amazonaws.com:~/
```

Change Spark python version to python 3
```bash
sudo sed -i -e '$a\export PYSPARK_PYTHON=/usr/bin/python3' /etc/spark/conf/spark-env.sh
```

Optionally change 

    log4j.rootCategory=INFO, console
    
to

    log4j.rootCategory=ERROR, console
    
in /usr/lib/spark/conf/log4j.properties to disable INFO and WARNING logs.

#### Execution

Access the cluster's master by running:

```bash
ssh -i ./capstone_project.pem hadoop@ec2-**-***-***-***.us-west-2.compute.amazonaws.com
```

Run the script inside EMR cluster with:

```bash
spark-submit --packages saurfang:spark-sas7bdat:2.0.0-s_2.10 etl.py
```