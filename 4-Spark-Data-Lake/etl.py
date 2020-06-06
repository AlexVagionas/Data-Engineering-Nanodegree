import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, date_format
from pyspark.sql.types import StructType as Struct, StructField as Fld, DoubleType as Double, \
    StringType as Str, IntegerType as Int, LongType as Long

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Processes song data and stores them as parquet files

    Loads song data into a spark DataFrame and transforms them into songs
    and artists DataFrames which are subsequently written as parquet files
    to songs and artists folders in the specified output path.

    Parameters:
    spark : SparkSession instance
    input_data (str) : Path of the directory of song_data
    output_data (str) : Path of the directory where the parquet files will be stored

    """

    # specify schema for song data
    songs_schema = Struct([
        Fld('num_songs', Int()),
        Fld('artist_id', Str()),
        Fld('artist_latitude', Double()),
        Fld('artist_longtitude', Double()),
        Fld('artist_location', Str()),
        Fld('artist_name', Str()),
        Fld('song_id', Str()),
        Fld('title', Str()),
        Fld('duration', Double()),
        Fld('year', Int())
    ])

    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'

    # read song data file
    df = spark.read.json(song_data, songs_schema)

    # extract columns to create songs table
    songs_table = df[['song_id', 'title', 'artist_id', 'year', 'duration']] \
        .dropDuplicates(['song_id'])

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(os.path.join(output_data, 'songs'),
                              'overwrite', partitionBy=['year', 'artist_id'])

    # extract columns to create artists table
    artists_table = df[['artist_id', 'artist_name', 'artist_location',
                        'artist_latitude', 'artist_longtitude']].dropDuplicates(['artist_id'])

    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists'), 'overwrite')


def process_log_data(spark, input_data, output_data):
    """
    Processes log data and stores them as parquet files

    Loads log data into a spark DataFrame and transforms them into users, time
    and songplays DataFrames which are subsequently written as parquet files
    to users, time and songplays folders in the specified output path.

    Parameters:
    spark : SparkSession instance
    input_data (str) : Path of the directory of log_data
    output_data (str) : Path of the directory where the parquet files will be stored

    """

    # get filepath to log data file
    log_data = input_data + 'log-data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # cast userId column to integer
    df = df.withColumn('userId', df['userId'].cast('integer'))

    # extract columns for users table
    users_table = df[['userId', 'firstName', 'lastName', 'gender', 'level']] \
        .dropDuplicates(['userId'])

    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users'), 'overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: x / 1000, Double())
    df = df.withColumn('startTime', get_timestamp(df.ts))

    # create datetime column from original timestamp column
    df = df.withColumn('datetime', df.startTime.cast('timestamp'))

    # extract columns to create time table
    time_table = df[df.startTime,
                    hour(df.datetime).alias('hour'),
                    dayofmonth(df.datetime).alias('day'),
                    weekofyear(df.datetime).alias('week'),
                    month(df.datetime).alias('month'),
                    year(df.datetime).alias('year'),
                    dayofweek(df.datetime).alias('weekday')]

    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(os.path.join(output_data, 'time'),
                             'overwrite', partitionBy=['year', 'month'])

    # specify schema for song data
    songs_schema = Struct([
        Fld('num_songs', Int()),
        Fld('artist_id', Str()),
        Fld('artist_latitude', Double()),
        Fld('artist_longtitude', Double()),
        Fld('artist_location', Str()),
        Fld('artist_name', Str()),
        Fld('song_id', Str()),
        Fld('title', Str()),
        Fld('duration', Double()),
        Fld('year', Int())
    ])

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + 'song_data/*/*/*/*.json', songs_schema)

    # specify join conditions and columns of songplays table
    songplays_cols = ('datetime', 'userId', 'level', 'song_id',
                      'artist_id', 'sessionId', 'location', 'userAgent')
    conditions = [df.song == song_df.title, df.artist ==
                  song_df.artist_name, df.length == song_df.duration]

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = df.join(song_df, conditions).select(*songplays_cols)

    # add month and year columns
    songplays_table = songplays_table.withColumn('month', month(df.datetime))
    songplays_table = songplays_table.withColumn('year', year(df.datetime))

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(os.path.join(output_data, 'songplays'),
                                  'overwrite', partitionBy=['year', 'month'])


def main():
    """
    Loads and processes the data from Udacity's S3 and outputs them in the specified S3 data lake

    """

    spark = create_spark_session()

    input_data = "s3a://udacity-dend/"
    output_data = "s3a://alex-dend-dlake"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
