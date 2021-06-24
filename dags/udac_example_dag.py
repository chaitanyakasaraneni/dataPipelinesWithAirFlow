from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                               LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# In the DAG, add default parameters according to these guidelines
default_args = {
    'owner'          : 'chaitanya',
    'start_date'     : datetime(2018, 11, 1),
    'end_date'       : datetime(2018, 11, 1),
    'depends_on_past': False, # The DAG does not have dependencies on past runs
    'retries'        : 3, # On failure, the task are retried 3 times
    'retry_delay'    : timedelta(minutes = 5), # Retries happen every 5 minutes
    'catchup'        : False, # Catchup is turned off
    'email_on_retry' : False # Do not email on retry
}

dag = DAG(
    dag_id              = 'sparkify',
    default_args        = default_args,
    description         = 'Load and transform data in Redshift with Airflow',
    template_searchpath = ['/home/workspace/airflow/'], #https://stackoverflow.com/a/52691700
    schedule_interval   = '0 * * * *'
)

start = DummyOperator(task_id = 'ETL_Start',  dag = dag)


stage_events = StageToRedshiftOperator(
    task_id          = 'Stage_Events',
    table            = 'staging_events',
    s3_key           = 'log-data/{execution_date.year}/{execution_date.month:02d}', #2-digit month
    file_format      = "JSON 's3://udacity-dend/log_json_path.json'",
    provide_context  = True,
    dag              = dag
)

stage_songs = StageToRedshiftOperator(
    task_id          = 'Stage_Songs',
    table            = 'staging_songs',
    s3_key           = 'song_data', # Add /A/B (588 songs) to make it run faster
    file_format      = "JSON 'auto'",
    provide_context  = True,
    dag              = dag
)

load_songplays = LoadFactOperator(
    task_id          = 'Load_Songplays',
    table            = 'songplays',
    sql_query        = SqlQueries.songplay_table_insert,
    dag              = dag
)

load_users = LoadDimensionOperator(
    task_id          = 'Load_Dimension_Users',
    table            = 'users',
    sql_query        = SqlQueries.user_table_insert,
    dag              = dag
)

load_songs = LoadDimensionOperator(
    task_id          = 'Load_Dimension_Songs',
    table            = 'songs',
    sql_query        = SqlQueries.song_table_insert,
    dag              = dag
)

load_artists = LoadDimensionOperator(
    task_id          = 'Load_Dimension_Artists',
    table            = 'artists',
    sql_query        = SqlQueries.artist_table_insert,
    dag              = dag
)

load_time = LoadDimensionOperator(
    task_id          = 'Load_Dimension_Time',
    table            = 'time',
    sql_query        = SqlQueries.time_table_insert,
    dag              = dag
)

quality_checks = DataQualityOperator(
    task_id = 'Quality_Checks',
    queries = ({'sql':'SELECT COUNT(*) FROM songs   WHERE title IS NULL', 'expect':0},
               {'sql':'SELECT COUNT(*) FROM users   WHERE level IS NULL', 'expect':0},
               {'sql':'SELECT COUNT(*) FROM artists WHERE  name IS NULL', 'expect':0}),
    dag     = dag
)

end = DummyOperator(task_id = 'ETL_Stop',  dag = dag)

start >> [stage_events, stage_songs] >> load_songplays
load_songplays >> [load_users, load_songs, load_artists, load_time] >> quality_checks
quality_checks >> end