from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 checks,
                 redshift_conn_id='redshift',
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.checks = checks

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)

        for check in self.checks:
            query = check['query']
            expected_result = check['expected_result']

            results = redshift.get_records(query)

            self.log.info(f'Results {results}')

            if results and results[0][0] != expected_result:
                raise ValueError(
                    f"Data quality check failed. "
                    f"The query `{query}` returned {expected_result} records")
