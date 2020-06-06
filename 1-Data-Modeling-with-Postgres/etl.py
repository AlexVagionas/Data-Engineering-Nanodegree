import calendar
import glob
import os

import pandas as pd
import psycopg2

from sql_queries import *

# global variables

MILLISECONDS_IN_SECOND = 1000


def process_song_file(cur, filepath):
    """
    Extracts the contents of a .json song file and inserts them into songs and artists tables.

    Parameters:
    cur : Cursor object instance which allows execution of PostgresSQL commands
    filepath (str) : Path of the .json song file containing the data

    """

    # open song file
    df = pd.read_json(filepath, lines=True)

    # get DataFrame columns
    columns = df.columns

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0].tolist()

    cur.execute(song_table_insert, song_data)

    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location',
                      'artist_latitude', 'artist_longitude']].values[0].tolist()

    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Extracts the contents of a .json log file and inserts them into time, users and songplays tables.

    Parameters:
    cur : Cursor object instance which allows execution of PostgresSQL commands
    filepath (str) : Path of the .json log file containing the data

    """

    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df.loc[df['page'] == 'NextSong']

    # create datetime column from timestamp
    df['datetime'] = pd.to_datetime(df['ts'], unit='ms')

    # assign datetime column to new variable
    t = df['datetime']

    # insert time data records
    time_data = [[t[i], t.dt.hour[i], t.dt.day[i], t.dt.weekofyear[i],
                  t.dt.month[i], t.dt.year[i], calendar.day_name[t.dt.dayofweek[i]]] for i in t.index]
    column_labels = ['timestamp', 'hour', 'day', 'week of year', 'month', 'year', 'weekday']
    time_df = pd.DataFrame(time_data, columns=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['registration', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        # insert data to songplays table only if the songid and artistid are not None
        if results:
            songid, artistid = results
        else:
            continue

        # insert songplay record
        songplay_data = [row.datetime, row.userId, row.level, songid,
                         artistid, row.sessionId, row.location, row.userAgent]
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Iterates over all files in a directory and processes their data using a specified function (func).

    Parameters:
    cur : Cursor object instance which allows execution of PostgresSQL commands
    conn : Instance of a connection to a PostgresSQL database
    filepath (str) : Path of the directory which contains all the data files to be processed
    func : function to be used for processing of the data

    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
