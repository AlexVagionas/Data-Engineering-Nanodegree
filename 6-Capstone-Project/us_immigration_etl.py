import os

from itertools import chain
from pyspark.sql.functions import when, col, create_map, lit, count, year, month


def process_immigration_data(spark, input_data, output_data, cit_res, port_state_code, port_city, mode, addr, visa):
    """
    ETL process for i94_{mon}16_sub.sas7bdat datasets

    Parameters:
    spark (SparkSession) : Spark Session
    input_data (str) : path of input data
    output_data (str) : path of directory where output data will be stored
    cit_res (dict) : Mapping for i94cit and i94res columns' values
    port_state_code (dict) : Mapping for i94port column's values -state part-
    port_city (dict) : Mapping for i94port column's values -city part-
    mode (dict) : Mapping for i94mode column's values
    addr (dict) : Mapping for i94addr column's values
    visa (dict) : Mapping for i94visa column's values

    """

    # read data from each dataset and compine than to a single DataFrame
    df = spark.read.format('com.github.saurfang.sas.spark').load(input_data[0])
    for path in input_data[1:]:
        df = df.union(spark.read.format('com.github.saurfang.sas.spark').load(path))

    # drop columns that won't be used
    cols_to_drop = ['_c0', 'cicid', 'count', 'visapost', 'occup',
                    'entdepa', 'entdepd', 'entdepu', 'matflag', 'insnum']
    df = df.drop(*cols_to_drop)

    # replace invalid state codes with '99'
    df = df.withColumn('i94addr', when(~df['i94addr'].isin(
        *(addr.keys())), '99').otherwise(df['i94addr']))

    # (mapping dictionary, column where mapping is applied, new column name)
    maps = [(cit_res, 'i94cit', 'i94cit'), (cit_res, 'i94res', 'i94res'),
            (port_state_code, 'i94port', 'i94port_state'), (port_city, 'i94port', 'i94port_city'),
            (mode, 'i94mode', 'i94mode'), (addr, 'i94addr', 'state'), (visa, 'i94visa', 'i94visa')]

    # use mappings to replace codes in 'i94cit', 'i94res', 'i94port', 'i94mode',
    # 'i94addr' and 'i94visa' columns with their values
    for map_dic, from_col, col_name, in maps:
        mapping_expr = create_map([lit(x) for x in chain(*map_dic.items())])
        df = df.withColumn(col_name, mapping_expr.getItem(col(from_col)))

    df.createOrReplaceTempView('immigration')

    # transform gender column values: 'M' is replaced with 'Male' and 'F' with 'Female'
    # if the transportation mode is 'Land', 'Sea', 'Not reported' of NULL and
    # flight number or airline is not NULL: transportation mode is changes to 'Air'
    df = spark.sql("""
        SELECT CAST(i94yr AS INT) AS arrival_year,
               CAST(i94mon AS INT) AS arrival_month,
               DATE_ADD('1960-01-01', arrdate) AS arrival_date,
               DATE_ADD('1960-01-01', depdate) AS departure_date,

               i94port_city AS port_city,
               i94port_state AS port_state_code,

               i94cit AS origin_country,
               i94res AS residence_country,
               CAST(biryear AS INT) AS birth_year,
               CAST(i94bir AS INT) AS age,
               CASE
                    WHEN gender = 'M' THEN 'Male'
                    WHEN gender = 'F' THEN 'Female'
               END AS gender,

               CAST(admnum AS INT) AS admission_num,
               TO_DATE(dtadfile, 'yyyyMMdd') AS admission_date,
               TO_DATE(dtaddto, 'MMddyyyy') AS admitted_until,
               i94visa visa_category,
               visatype AS visa_type,

               state,
               i94addr AS state_code,

               CASE
                    WHEN (i94mode = 'Land' AND ((fltno IS NOT NULL) OR (airline IS NOT NULL))) THEN 'Air'
                    WHEN (i94mode = 'Sea' AND ((fltno IS NOT NULL) OR (airline IS NOT NULL))) THEN 'Air'
                    WHEN (i94mode = 'Not reported' AND ((fltno IS NOT NULL) OR (airline IS NOT NULL))) THEN 'Air'
                    WHEN (i94mode IS NULL AND ((fltno IS NOT NULL) OR (airline IS NOT NULL))) THEN 'Air'
                    ELSE i94mode
               END AS transportation_mode,
               airline,
               fltno AS flight_num
          FROM immigration
    """)

    df = df.distinct()

    # data_quality_check(df)

    # save DataFrame as .parquet in output_data/us_immigration directory
    print('Saving us_immigration table to {}'.format(output_data))
    df.write.parquet(os.path.join(output_data, 'us_immigration'), 'overwrite',
                     partitionBy=['arrival_month', 'port_state_code'])


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

    if df.filter((df.transportation_mode.isin('Land', 'Sea')) &
                 ((df.flight_num.isNotNull()) | (df.airline.isNotNull()))
                 ).count() > 0:
        print('Invalid transportation mode entries in dataset')

    if df.filter(((df.transportation_mode == 'Not reported') | (df.transportation_mode.isNull())) &
                 ((df.flight_num.isNotNull()) | (df.airline.isNotNull()))
                 ).count() > 0:
        print('Invalid transportation mode entries in dataset')

    if df.filter(df.arrival_year != 2016).count() > 0:
        print('Invalid year entries in dataset')

    if df.filter((df.arrival_month <= 1) & (df.arrival_month >= 12)).count() > 0:
        print('Invalid month entries in dataset')

    if df.filter(df.arrival_year != year(df.arrival_date)).count() or \
       df.filter(df.arrival_month != month(df.arrival_date)).count():
        print('Invalid arrival date entries in dataset')

    print('All data quality checks have been executed.')
