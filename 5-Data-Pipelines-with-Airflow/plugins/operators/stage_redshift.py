from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):

    ui_color = '#358140'
    template_fields = ('s3_key',)

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 json="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json = json
        self.aws_credentials_id = aws_credentials_id

    def execute(self, context):
        self.log.info(f'Staging to {self.table} begins')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f'Clearing data from {self.table} table')
        redshift.run(f'DELETE FROM {self.table}')

        self.log.info('Rendering S3 path')
        s3_key_args = {'execution_date': str(context['execution_date'].date()),
                       'execution_year': context['execution_date'].year,
                       'execution_month': context['execution_date'].month}
        rendered_key = self.s3_key.format(**s3_key_args)
        s3_path = f's3://{self.s3_bucket}/{rendered_key}'

        self.log.info(f'Copying data from {s3_path} to Redshift')
        sql = (f"""
            COPY {self.table}
            FROM '{s3_path}'
            ACCESS_KEY_ID '{credentials.access_key}'
            SECRET_ACCESS_KEY '{credentials.secret_key}'
            JSON '{self.json}';
        """)
        redshift.run(sql)

        self.log.info(f'Staging to {self.table} finished')
