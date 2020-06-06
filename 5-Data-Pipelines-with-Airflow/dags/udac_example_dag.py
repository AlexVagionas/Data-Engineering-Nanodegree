from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator, DataQualityOperator)

from subdag import load_dimension_tables_dag
from helpers import SqlQueries

AWS_KEY = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'alexander',
    'start_date': datetime(2018, 11, 1),
    'end_date': datetime(2018, 11, 30),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG('data_pipeline_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@daily',
          max_active_runs=1
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table='staging_events',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='udacity-dend',
    s3_key='log_data/{execution_year}/{execution_month}/{execution_date}-events.json',
    json='s3://udacity-dend/log_json_path.json',
    provide_context=True,
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table='staging_songs',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='udacity-dend',
    s3_key='song_data',
    json='auto',
    provide_context=True
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift',
    sql_query=SqlQueries.songplay_table_insert,
    table='songplays'
)

load_dim_tables = SubDagOperator(
    subdag=load_dimension_tables_dag(
        'data_pipeline_dag',
        'Load_dim_tables_subdag',
        'redshift',
        [SqlQueries.user_table_insert, SqlQueries.song_table_insert,
         SqlQueries.artist_table_insert, SqlQueries.time_table_insert],
        ['users', 'songs', 'artists', 'time'],
        [False, False, False, False],
        default_args=default_args
    ),
    task_id='Load_dim_tables_subdag',
    dag=dag,
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    data_checks=[
        {'check_sql': 'SELECT COUNT(*) FROM songplays WHERE level NOT IN (\'free\', \'paid\')',
         'expected_result': 0},
        {'check_sql': 'SELECT COUNT(*) FROM songplays WHERE songid IS NULL', 'expected_result': 0},
        {'check_sql': 'SELECT COUNT(*) FROM songplays WHERE artistid IS NULL',
         'expected_result': 0},
        {'check_sql': 'SELECT COUNT(*) FROM songplays WHERE sessionid IS NULL',
         'expected_result': 0},
        {'check_sql': 'SELECT COUNT(*) FROM songs WHERE title IS NULL', 'expected_result': 0},
        {'check_sql': 'SELECT COUNT(*) FROM songs WHERE duration <= 0', 'expected_result': 0},
        {'check_sql': 'SELECT COUNT(*) FROM artists WHERE name IS NULL', 'expected_result': 0},
        {'check_sql': 'SELECT COUNT(*) FROM users WHERE first_name IS NULL', 'expected_result': 0},
        {'check_sql': 'SELECT COUNT(*) FROM users WHERE last_name IS NULL', 'expected_result': 0},
        {'check_sql': 'SELECT COUNT(*) FROM users WHERE gender NOT IN (\'F\', \'M\')',
         'expected_result': 0},
        {'check_sql': 'SELECT COUNT(*) FROM users WHERE level NOT IN (\'free\', \'paid\')',
         'expected_result': 0}
    ]
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]

[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table

load_songplays_table >> load_dim_tables

load_dim_tables >> run_quality_checks

run_quality_checks >> end_operator
