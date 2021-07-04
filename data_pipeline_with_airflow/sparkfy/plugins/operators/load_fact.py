from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'

    template_fields = ("execution_date",)

    @apply_defaults
    def __init__(self,
                 query,
                 table_name,
                 redshift_conn_id='redshift',
                 execution_date=None,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.query = query
        self.redshift_conn_id = redshift_conn_id
        self.execution_date = execution_date
        self.table_name = table_name

    def execute(self, context):
        where = ''
        if self.execution_date:
            self.log.info(f'Processing facts only for {self.execution_date}')
            where = f'WHERE start_date = {self.execution_date}'

        redshift = PostgresHook(self.redshift_conn_id)
        try:
            sql = '''
                BEGIN;

                INSERT INTO {table_name}
                SELECT * FROM (
                    {query}
                );

                COMMIT;
                '''.format(
                    table_name=self.table_name,
                    query=self.query,
                    where=where
                )
            redshift.run(sql)
        except:
            self.log.error(f'Cannot persist {self.table_name} values')
            if redshift:
                redshift.run('ROLLBACK;')
