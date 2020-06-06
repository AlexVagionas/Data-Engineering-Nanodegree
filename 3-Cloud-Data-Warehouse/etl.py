import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Loads data into staging events and songs tables.

    Copies the events and songs data from Amazon S3 buckets
    specified in dwh.cfg. These data are stored in JSON format.

    Parameters:
    cur : Cursor object instance which allows execution of SQL commands
    conn : Instance of a connection to Redshift database

    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Inserts data into fact and dimension tables.

    Transforms data stored in staging table into a star schema
    with 1 fact table, songplays, and 4 dimension tables, users,
    songs, artists and time.

    Parameters:
    cur : Cursor object instance which allows execution of SQL commands
    conn : Instance of a connection to Redshift database

    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Creates a star schema suitable for analytics from song and log datasets.

    Connects to the Redshift database specified in dwh.cfg file and loads
    the song and log datasets to their respective staging tables. Then, the
    data are inserted into fact and dimension tables comprising a star schema.
    In the end, the connection to Redshift database is closed.

    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values())
    )
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
