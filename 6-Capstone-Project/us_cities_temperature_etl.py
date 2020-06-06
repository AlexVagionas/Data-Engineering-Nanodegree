import os

from pyspark.sql.types import StructType as R, StructField as Fld, StringType as Str, DateType as Date, DecimalType as Dec
from pyspark.sql.functions import count, when, col


def process_cities_temperature_data(spark, input_data, output_data):
    """
    ETL process for GlobalLandTemperaturesByCity.csv dataset

    Parameters:
    spark (SparkSession) : Spark Session
    input_data (str) : path of input data
    output_data (str) : path of directory where output data will be stored

    """
    # define custom schema for DataFrame
    temperature_schema = R([
        Fld('dt', Date()),
        Fld('AverageTemperature', Dec(4, 2)),
        Fld('AverageTemperatureUncertainty', Dec(4, 2)),
        Fld('City', Str()),
        Fld('Country', Str()),
        Fld('Latitude', Str()),
        Fld('Longitude', Str())
    ])

    # read data with custom schema
    df = spark.read.csv(input_data, header=True, schema=temperature_schema)

    df.createOrReplaceTempView('temperatures')

    # transform dataset so that only measuments from 2000 to 2012 for U.S. cities are kept
    # converted city names to uppercase and replac SAINT prefix with ST
    df = spark.sql("""
        SELECT REPLACE(UPPER(City), 'SAINT', 'ST') AS city,
               CAST(AVG(AverageTemperature) AS DECIMAL(4,2)) AS average_temperature,
               CAST(AVG(AverageTemperatureUncertainty) AS DECIMAL(4,2)) AS average_temperature_uncertainty
          FROM (
                 SELECT *, YEAR(dt) AS year, MONTH(dt) AS month
                   FROM temperatures
               )
         WHERE Country = "United States" AND year >= 2000 AND year < 2013
         GROUP BY City
         ORDER BY City
    """)

    data_quality_check(df)

    # save DataFrame as .parquet in output_data/us_cities_temperature directory
    print('Saving us_cities_temperature table to {}'.format(output_data))
    df.write.parquet(os.path.join(output_data, 'us_cities_temperature'), 'overwrite')


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
