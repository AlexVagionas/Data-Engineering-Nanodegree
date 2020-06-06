# Data Lake

## Introduction

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. Currently, the data resides inside Amazon's storage infrastructure as a directory of JSON logs on user activity and a directory of JSON metadata on the songs.

In this project, an ETL pipeline is created using Spark running on a cluster created with EMR Amazon Web Service in order to transform the data into a form suitable for analytics.

## Setup of necessary AWS

To process the data, an AWS EMR cluster is created with the following setting:
- Release: `emr-5.29.0`
- Applications: `Spark`: Spark 2.4.4 on Hadoop 2.8.5 YARN with Ganglia 3.7.2 and Zeppelin 0.8.2
- Instance type: `m5.xlarge`
- Number of instance: `3`
- EC2 key pair: `data-lake`

The EC2 key pair is needed in order to ssh into the cluster's master node so it should be created before the cluster.

To store the processed data, a S3 bucket called **alex-dend-dlake** is created. An AWS user with permission to read and write to S3 is also needed. The user's access key and secret should be writte in the dl.cfg file.

## ETL process

The datasets are loaded into Spark DataFrames and subsequently are transformed into a fact and 4 dimension tables which are stored as **parquet** files in **alex-dend-dlake** S3 bucket.

The star schema consists of the following tables:

#### Fact table
1. **songplays** - records in log data associated with song plays
   - *datetime, user_id, level, song_id, artist_id, session_id, location, user_agent, month, year*

#### Dimension tables
2. **users** - users in the app
   - *userId, firstName, lastName, gender, level*
3. **songs** - songs in music database
   - *song_id, title, artist_id, year, duration*
4. **artists** - artists in music database
   - *artist_id, name, location, latitude, longitude*
5. **time** - timestamps of records in songplays broken down into specific units
   - *startTime, hour, day, week, month, year, weekday*

## Files Used

`data-lake.pem` Contains the EC2 secret key
`etl.py` Loads the datasets from S3 to Spark DataFrames, transform them into the start schema and store the results in S3 bucket as parquet files
`dl.cfg` AWS user's access key and secret

## Usage

To copy the `etl.py` and `dl.cfg` files to the cluster open a terminal and run

```bash
scp -i data-lake.pem etl.py dl.cfg hadoop@ec2-**-***-***-***.us-west-2.compute.amazonaws.com:~/
```

To get access to a terminal in cluster's master run:

```bash
ssh -i ~/data-lake.pem hadoop@ec2-**-***-***-***.us-west-2.compute.amazonaws.com
```

To run the script inside the cluster:

```bash
spark-submit etl.py
```
