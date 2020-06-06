from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql_query="",
                 table="",
                 delete_insert=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.table = table
        self.delete_insert = delete_insert

    def execute(self, context):
        self.log.info(f'Loading of dimension table {self.table} begins')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.delete_insert:
            self.log.info(f'Erasing data from {self.table} table')
            redshift.run(f'DELETE FROM {self.table}')

        self.log.info(f'Inserting data into {self.table} table')
        redshift.run(f'INSERT INTO {self.table} {self.sql_query}')

        self.log.info(f'Loading of dimension table {self.table} finished')
