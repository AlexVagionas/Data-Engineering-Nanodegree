import configparser
import os

from pyspark.sql import SparkSession

from sas_labels_processing import process_sas_mappings
from us_immigration_etl import process_immigration_data
from us_cities_demographics_etl import process_us_cities_demographics_data
from us_cities_temperature_etl import process_cities_temperature_data
from state_airport_etl import process_airport_data

config = configparser.ConfigParser()
config.read('aws.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Creates a Spark Session

    Returns:
    spark (SparkSession) : Spark Session

    """
    spark = SparkSession \
        .builder \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.0') \
        .config('spark.jars.packages', 'saurfang:spark-sas7bdat:2.0.0-s_2.11') \
        .appName('us_immigration_project') \
        .getOrCreate()

    print('Spark Session created.')

    return spark


def main():
    """
    ETL process to bring the data to Star Schema
    and save them as .parquet in Amazon S3.

    """
    print('ETL process begins!')

    spark = create_spark_session()

    # S3 bucket
    s3_bucket = config['AWS']['S3']

    # select which months to process for immigration data
    months = ['jan', 'feb']
    # months = ['jan', 'feb', 'mar', 'apr', 'may', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']

    # paths of input datasets
    immigration_data_paths = [month.join(
        [s3_bucket + 'datasets/i94_', '16_sub.sas7bdat']) for month in months]
    us_cities_demographics_data_path = s3_bucket + 'datasets/us-cities-demographics.csv'
    temperature_data_path = s3_bucket + 'datasets/GlobalLandTemperaturesByCity.csv'
    airport_data_path = s3_bucket + 'datasets/airport-codes_csv.csv'

    # path where output will be stored
    out_path = s3_bucket + 'output/'

    # get mappings from I94_SAS_Labels_Descriptions.SAS
    cit_res, port_state_code, port_city, mode, addr, visa = process_sas_mappings()

    # ETL process for input datasets

    print('-'*20)
    print('Processing us immigration dataset...')
    process_immigration_data(spark, immigration_data_paths, out_path,
                             cit_res, port_state_code, port_city, mode, addr, visa)
    print('Finished processing us immigration data.')

    print('-'*20)
    print('Processing us cities demographics dataset...')
    process_us_cities_demographics_data(spark, us_cities_demographics_data_path, out_path)
    print('Finished processing us cities demographics data.')

    print('-'*20)
    print('Processing cities temperature dataset...')
    process_cities_temperature_data(spark, temperature_data_path, out_path)
    print('Finished processing cities temperature data.')

    print('-'*20)
    print('Processing airport codes dataset...')
    process_airport_data(spark, airport_data_path, out_path, addr)
    print('Finished processing airport codes data.')

    print('-'*20)
    print('ETL process finished!')


if __name__ == "__main__":
    main()
