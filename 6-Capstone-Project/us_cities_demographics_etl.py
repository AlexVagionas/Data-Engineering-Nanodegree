import os

from pyspark.sql.functions import first, col, coalesce, lit, count, when


def process_us_cities_demographics_data(spark, input_data, output_data):
    """
    ETL process for us-cities-demographics.csv dataset

    Parameters:
    spark (SparkSession) : Spark Session
    input_data (str) : path of input data
    output_data (str) : path of directory where output data will be stored

    """
    # read data
    df = spark.read.csv(input_data, header=True, sep=';')

    df.createOrReplaceTempView('us_cities_demographics')

    # converted city names to uppercase and replac SAINT prefix with ST
    df = spark.sql("""
        SELECT REPLACE(UPPER(City), 'SAINT', 'ST') AS city,
               `State Code` AS state_code,
               State AS state,
               CAST(`Total Population` AS INT) AS total_population,
               CAST(`Male Population` AS INT) AS male_population,
               CAST(`Female Population` AS INT) AS female_population,
               CAST(`Median Age` AS DOUBLE) AS median_age,
               CAST(`Average Household Size` AS DOUBLE) AS average_household_size,
               CAST(`Number of Veterans` AS INT) AS number_of_veterans,
               CAST(`Foreign-born` AS INT) AS foreign_born,
               Race AS race,
               CAST(Count AS INT) AS count
          FROM us_cities_demographics
    """)

    group_by_cols = [col for col in df.columns if col not in ['race', 'count']]

    # 'race' column is used as a pivot so that each row, which represents
    # a city, has one column for every race
    df = df.groupBy(*group_by_cols).pivot('race').agg(first('count'))

    # if a race does not exist, convert nulls to 0
    df = df.withColumn('american_indian_and_alaska_native', coalesce(df['American Indian and Alaska Native'], lit(0))) \
           .withColumn('asian',                             coalesce(df['Asian'],                             lit(0))) \
           .withColumn('black_or_african_american',         coalesce(df['Black or African-American'],         lit(0))) \
           .withColumn('hispanic_or_latino',                coalesce(df['Hispanic or Latino'],                lit(0))) \
           .withColumn('white',                             coalesce(df['White'],                             lit(0))) \
           .drop('American Indian and Alaska Native') \
           .drop('Black or African-American') \
           .drop('Hispanic or Latino')

    df = df.orderBy('state_code', 'city')

    data_quality_check(df)

    # save DataFrame as .parquet in output_data/us_cities_demographics directory
    print('Saving us_cities_demographics table to {}'.format(output_data))
    df.write.parquet(os.path.join(output_data, 'us_cities_demographics'), 'overwrite')


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

    if df.filter((df.male_population + df.female_population - df.total_population) != 0).count() > 0:
        print('Total Population not equal to Male + Female Population')

    print('All data quality checks have been executed.')
