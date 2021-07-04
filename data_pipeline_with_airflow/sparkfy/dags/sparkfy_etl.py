from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (
    StageToRedshiftOperator,
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator
)

from helpers import SqlQueries

log_data = 's3://udacity-dend/log_data'
song_data = 's3://udacity-dend/song_data'

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2021, 6, 27),
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('sparkfy_etl',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          catchup = False
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables_task = PostgresOperator(
    task_id="create_tables",
    dag=dag,
    sql='create_tables.sql',
    postgres_conn_id="redshift"
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    origin=log_data,
    destination='public.staging_events',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    json_format="s3://udacity-dend/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',  
    dag=dag,
    origin=song_data,
    destination='public.staging_songs',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    query=SqlQueries.songplay_table_insert,
    table_name='public.songplays',
    redshift_conn_id='redshift',
    execution_date='{{ execution_date }}'
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    query=SqlQueries.user_table_insert,
    table_name='public.users',
    recreate=True,
    redshift_conn_id='redshift'
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    query=SqlQueries.song_table_insert,
    table_name='public.songs',
    recreate=True,
    redshift_conn_id='redshift'
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    query=SqlQueries.artist_table_insert,
    table_name='public.artists',
    recreate=True,
    redshift_conn_id='redshift'
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    query=SqlQueries.time_table_insert,
    table_name='public."time"',
    recreate=True,
    redshift_conn_id='redshift'
)

# Create pairs for query:expected_result to validate cases where the id is null
dq_queries = [
    SqlQueries.dq_users_not_null, 
    SqlQueries.dq_songs_not_null,
    SqlQueries.dq_artists_not_null, 
    SqlQueries.dq_time_not_null,
    SqlQueries.dq_songplays_not_null
]
dq_checks = [{'query': q, 'expected_result': 0} for q in dq_queries]

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    checks=dq_checks,
    redshift_conn_id='redshift',
    execution_date='{{ execution_date }}'
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_tables_task
create_tables_task >> stage_events_to_redshift >> load_songplays_table
create_tables_task >> stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_user_dimension_table >> run_quality_checks
load_songplays_table >> load_song_dimension_table >> run_quality_checks
load_songplays_table >> load_artist_dimension_table >> run_quality_checks
load_songplays_table >> load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator
