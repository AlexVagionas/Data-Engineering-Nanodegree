# Data Modeling with Postgres

## Introduction

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. Currently, the data resides in a directory of JSON logs on user activity, as well as a directory with JSON metadata on the songs.

In this project, a database schema and ETL pipeline are created using PostgresSQL and python to easily query the data and run analytics.

## Implementation

For the database, a star schema is choosen with the following tables:

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

`log_data` Directory where the logs of user activity are stored in .JSON files
`song_data` Directory where metadata of songs are stored in .JSON files
`sql_queries.py` Contains SQL queries used by the next 2 files
`create_tables.py` Drops and creates the database with the tables
`etl.py` Reads and processes files from `song_data` and `log_data` and loads them into the tables.

## Usage

To create the database and the empty tables open a terminal and run:

```bash
python create_tables.py
```

If the database already exists it gets dropped and recreated.

To insert data from .JSON logs and metadata in the database run:

```bash
python etl.py
```
