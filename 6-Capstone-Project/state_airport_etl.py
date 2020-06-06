import os

from itertools import chain
from pyspark.sql.functions import first, col, create_map, lit, count, when


def process_airport_data(spark, input_data, output_data, addr):
    """
    ETL process for airport-codes_csv.csv dataset

    Parameters:
    spark (SparkSession) : Spark Session
    input_data (str) : path of input data
    output_data (str) : path of directory where output data will be stored
    addr (dict) : Mapping of state codes to state names

    """
    # read data
    df = spark.read.csv(input_data, header=True)

    df.createOrReplaceTempView('airports')

    # create new DataFrame containing the number of airports of each type in each state
    # non-U.S. airports are excluded
    df = spark.sql("""
        SELECT RIGHT(iso_region, LENGTH(iso_region) - 3) AS state_code, type, COUNT(*) AS airport_count
          FROM airports
         WHERE iso_country == "US" AND type != "closed"
         GROUP BY iso_region, type
         ORDER BY iso_region
    """)

    # 'type' column is used as a pivot so that each row, which represents
    # a state, has one column for every type of airport
    df = df.groupBy('state_code').pivot('type').agg(
        first('airport_count')).orderBy('state_code').fillna(0)

    mapping_expr = create_map([lit(x) for x in chain(*addr.items())])

    # create a column with state names by mapping state codes to state names
    df = df.withColumn('state', mapping_expr.getItem(col('state_code')))

    # drop rows with invalid states
    df = df.na.drop()

    data_quality_check(df)

    # save DataFrame as .parquet in output_data/state_airport directory
    print('Saving state_airport table to {}'.format(output_data))
    df.write.parquet(os.path.join(output_data, 'state_airport'), 'overwrite')


def data_quality_check(df):
    """
    Performs data quality checks in DataFrame

    Parameters:
    df (spark DataFrame) : DataFrame for which data quality checks shall be performed

    """
    print('Running data quality checks...')

    print('Total null values per column:')
    df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]) \
        .show(truncate=False, vertical=True)

    print('All data quality checks have been executed.')
