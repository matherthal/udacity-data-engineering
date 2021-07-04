from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 origin,
                 destination,
                 redshift_conn_id='redshift',
                 aws_credentials_id="aws_credentials",
                 json_format='auto',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.origin = origin
        self.destination = destination
        self.aws_credentials_id = aws_credentials_id
        self.json_format = json_format

    def execute(self, context):
        self.log.info(f'Copy files from {self.origin} to {self.destination}')

        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()

        redshift = PostgresHook(self.redshift_conn_id)

        copy_sql = (
            "COPY {destiny} \n"
            "FROM '{origin}' \n"
            "ACCESS_KEY_ID '{access_key_id}' \n"
            "SECRET_ACCESS_KEY '{secret_access_key}' \n"
            "COMPUPDATE OFF \n"
            "REGION 'us-west-2' \n"
            "FORMAT JSON '{json_format}';"
        ).format(
            destiny=self.destination,
            origin=self.origin,
            access_key_id=credentials.access_key,
            secret_access_key=credentials.secret_key,
            json_format=self.json_format
        )

        redshift.run(copy_sql)
