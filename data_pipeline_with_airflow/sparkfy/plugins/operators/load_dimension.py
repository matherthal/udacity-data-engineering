from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 query,
                 table_name,
                 recreate=False,
                 redshift_conn_id='redshift',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.query = query
        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.recreate = recreate

    def execute(self, context):
        self.log.info(f'Loading dimensions in table {self.table_name}')

        redshift = PostgresHook(self.redshift_conn_id)
        try:
            sql = 'BEGIN;'
            if self.recreate:
                sql += f'\nTRUNCATE {self.table_name};\n'

            sql += '''
            INSERT INTO {table_name}
            SELECT * FROM (
                {query}
            ) t;
            COMMIT;
            '''.format(
                    table_name=self.table_name,
                    query=self.query
                )
            redshift.run(sql)
        except:
            self.log.error(f'Cannot persist {self.table_name} values')
            if redshift:
                redshift.run('ROLLBACK;')
            raise
