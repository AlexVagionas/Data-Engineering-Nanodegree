from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql_query="",
                 table="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.table = table

    def execute(self, context):
        self.log.info(f'Loading of fact table {self.table} begins')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info('Inserting data into fact table')
        redshift.run(f'INSERT INTO {self.table} {self.sql_query}')

        self.log.info(f'Loading of fact table {self.table} finished')
