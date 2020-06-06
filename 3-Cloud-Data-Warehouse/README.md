# Data Warehouse

## Introduction

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. Currently, the data resides inside Amazon's storage infrastructure as a directory of JSON logs on user activity and a directory of JSON metadata on the songs.

In this project, an ETL pipeline is created using Amazon Web Services and python in order to transform the data into a form suitable for analytics.

## Implementation

A Redshift cluster is created to host the database along with an IAM role which has read-only access to S3 so that the datasets can be retrieved. The datasets are loaded into staging tables in Redshift and subsequently transformed into a fact and 4 dimension tables.

The star schema consists of the following tables:

#### Fact table
1. **songplays** - records in log data associated with song plays
   - *songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent*

#### Dimension tables
2. **users** - users in the app
   - *user_id, first_name, last_name, gender, level*
3. **songs** - songs in music database
   - *song_id, title, artist_id, year, duration*
4. **artists** - artists in music database
   - *artist_id, name, location, latitude, longitude*
5. **time** - timestamps of records in songplays broken down into specific units
   - *start_time, hour, day, week, month, year, weekday*

## Files Used

`init_redshift/init_redshift.py` Creates the Redshift cluster and IAM role
`init_redshift/verify_redshift.py` Verifies that the Redshift cluster is active and displays the endpoint and IAM role number
`init_redshift/init.cfg` Configurations for the creation of Redshift cluster and IAM role
`sql_queries.py` Contains SQL queries used by the next 2 files
`create_tables.py` Drops and creates the all the necessary tables in Redshift
`etl.py` Loads the datasets from S3 to the staging tables and transform them into the start schema
`dwh.cfg` Configurations for the connection to Redshift database (host, db_name, db_user, db_password, db_port and IAM role number) and S3 buckets (song_data and log_data)

## Usage

To create the Redshift cluster and IAM role run:

```bash
python init_redshift/init_redshift.py
```

After about 10 minutes, the cluster should become active. This can be verified by running:

```bash
python init_redshift/verify_redshift.py
```

If the cluster is active, the above script should also displays in console the endpoint and IAM role number which should be copied into dwh.cfg.

Then, to create the empty tables in Redshift run:

```bash
python create_tables.py
```

Finally, to copy the datasets into the staging tables and then transform the into the star schema run:

```bash
python etl.py
```
