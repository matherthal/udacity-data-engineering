from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 table_list,
                 redshift_conn_id='redshift',
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)

        self.table_list = table_list
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)

        for table_name in self.table_list:
            query = f'SELECT COUNT(1) FROM {table_name}'

            counts = redshift.get_records(query)
            
            self.log.info(f'Counts {counts}')

            if counts and counts[0][0] < 1:
                raise ValueError(
                    f"Data quality check failed. "
                    f"{table_name} returned {counts} records")
