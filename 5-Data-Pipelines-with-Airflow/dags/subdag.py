import datetime

from airflow import DAG
from airflow.operators.udacity_plugin import LoadDimensionOperator


def load_dimension_tables_dag(
        parent_dag_name,
        task_id,
        redshift_conn_id,
        queries,
        tables,
        delete_insert,
        *args, **kwargs):
    """
    Returns a DAG which loads users, songs, artists and time dimension tables.
    """

    dag = DAG(
        f'{parent_dag_name}.{task_id}',
        **kwargs
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        dag=dag,
        redshift_conn_id=redshift_conn_id,
        sql_query=queries[0],
        table=tables[0],
        delete_insert=delete_insert[0]
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        dag=dag,
        redshift_conn_id=redshift_conn_id,
        sql_query=queries[1],
        table=tables[1],
        delete_insert=delete_insert[1]
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        dag=dag,
        redshift_conn_id=redshift_conn_id,
        sql_query=queries[2],
        table=tables[2],
        delete_insert=delete_insert[2]
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        dag=dag,
        redshift_conn_id=redshift_conn_id,
        sql_query=queries[3],
        table=tables[3],
        delete_insert=delete_insert[3]
    )

    return dag
