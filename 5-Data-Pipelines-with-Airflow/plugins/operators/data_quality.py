from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 data_checks={},
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.data_checks = data_checks

    def execute(self, context):
        self.log.info('Data quality tests begin')

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        failed_tests = []
        error_count = 0

        for check in self.data_checks:
            sql = check.get('check_sql')
            exp_result = check.get('expected_result')

            records = redshift.get_records(sql)[0]

            if exp_result != records[0]:
                error_count += 1
                failed_tests.append(sql)

        if error_count > 0:
            self.log.info('Tests failed')
            self.log.info(failed_tests)
            raise ValueError('Data quality check failed')
