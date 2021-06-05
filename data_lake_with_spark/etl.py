import configparser
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType
import pyspark.sql.functions as F

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

LOGGER = None

def create_spark_session():
    """Get or create the spark session object

    Returns:
        SparkSession: The session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.3") \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    """Process the song data files from s3 to create the artist and song parquet
    tables

    Args:
        spark (SparkSession): The spark session object
        input_data (str): The input files path
        output_data (str): The output files path
    """
    # read song data file
    LOGGER.info('read song data file')
    df = spark.read.json(input_data)

    # extract columns to create songs table and drop duplicates
    LOGGER.info('extract columns to create songs table and drop duplicates')
    songs_table = df.select([
        'song_id', 'title', 'artist_id', 'year','duration']
    ).drop_duplicates(['song_id', 'artist_id'])

    # write songs table to parquet partitioned by year and artist
    LOGGER.info('write songs table to parquet partitioned by year and artist')
    songs_table.coalesce(1).write.mode('overwrite')\
        .partitionBy("year", "artist_id")\
        .parquet(os.path.join(output_data, 'song'))

    # extract columns to create artists table
    LOGGER.info('extract columns to create artists table')
    artists_table = df.select([
        'artist_id', 'artist_name', 'artist_location', 'artist_latitude', 
        'artist_longitude'
    ]).drop_duplicates(['artist_id'])

    # write artists table to parquet files
    LOGGER.info('write artists table to parquet files')
    artists_table.coalesce(1).write.mode('overwrite')\
        .parquet(os.path.join(output_data, 'artist'))


def process_log_data(spark, input_data, output_data):
    """Process user log data creating the tables user, time and songplays

    Args:
        spark (SparkSession): The spark session object
        input_data (str): The input files path
        output_data (str): The output files path
    """
    # read log data file
    LOGGER.info('read log data file')
    log_df = spark.read.json(input_data)

    # filter by actions for song plays
    LOGGER.info('filter by actions for song plays')
    log_df = log_df.where(F.col('page')=='NextSong')

    # extract columns for users table
    LOGGER.info('extract columns for users table')
    user_table = log_df.select([
        'userId', 'firstName', 'lastName', 'gender', 'level'])

    # write users table to parquet files
    LOGGER.info('write users table to parquet files')
    user_path = os.path.join(output_data, 'user')
    user_table.coalesce(1).write.mode('overwrite').parquet(user_path)

    # create datetime column from original timestamp column
    LOGGER.info('create datetime column from original timestamp column')
    get_timestamp = F.udf(
        lambda x : datetime.utcfromtimestamp(int(x)/1000), TimestampType())
    log_df = log_df.withColumn("start_time", get_timestamp("ts"))

    # extract columns to create time table
    LOGGER.info('extract columns to create time table')
    time_table = log_df.select(
        'start_time',
        F.hour('start_time').alias('hour'), 
        F.dayofmonth('start_time').alias('day'), 
        F.weekofyear('start_time').alias('weekofyear'),
        F.month('start_time').alias('month'),
        F.year('start_time').alias('year'),
        F.dayofweek('start_time').alias('weekday')
    ).drop_duplicates(['start_time'])

    # write time table to parquet partitioned by year and month
    LOGGER.info('write time table to parquet partitioned by year and month')
    time_table.coalesce(1).write.mode('overwrite')\
        .partitionBy('year', 'month')\
        .parquet(os.path.join(output_data, 'time'))

    # read in song data to use for songplays table
    LOGGER.info('read in song data to use for songplays table')
    song_df = spark.read.parquet(os.path.join(output_data, 'song'))
    artist_df = spark.read.parquet(os.path.join(output_data, 'artist'))

    # join artist and song data
    LOGGER.info('join artist and song data')
    song_df = artist_df.select(['artist_name', 'artist_id'])\
        .join(song_df, on='artist_id', how='inner')

    # extract columns from joined song and log datasets to create songplays
    LOGGER.info('extract columns from joined song and log datasets to create '
                'songplays')
    on_clause = \
        (song_df.title == log_df.song) \
        & (song_df.artist_name == log_df.artist) \
        & (song_df.duration == log_df.length)
    songplays_table = log_df.join(song_df, on_clause, how='inner')

    # select columns and create year and month columns
    LOGGER.info('select columns and create year and month columns')
    songplays_table = songplays_table.select(
        'start_time',
        F.col('userId').alias('user_id'),
        'level',
        'song_id',
        'artist_id',
        F.col('itemInSession').alias('session_id'),
        'location',
        F.col('userAgent').alias('user_agent'),
        F.month('start_time').alias('month'),
        F.year('start_time').alias('year'))

    # create songplay_id and drop duplicates by this column
    LOGGER.info('create songplay_id and drop duplicates by this column')
    key_columns = [
        'start_time', 'user_id', 'song_id', 'artist_id', 'session_id']
    songplays_table = songplays_table.withColumn(
        'songplay_id', 
        F.sha2(F.concat_ws("||", *key_columns), 256)
    ).drop_duplicates(['songplay_id'])

    # write songplays table to parquet files partitioned by year and month
    LOGGER.info('write songplays table to parquet partitioned by year/month')
    songplays_table.coalesce(1).write.mode('overwrite')\
        .partitionBy('year', 'month')\
        .parquet(os.path.join(output_data, 'songplays'))

def main():
    global LOGGER
    spark = create_spark_session()

    log4j_logger = spark.sparkContext._jvm.org.apache.log4j
    LOGGER = log4j_logger.LogManager.getLogger(__name__)

    try:
        input_log_data = config['PATHS']['INPUT_LOG_DATA']
        input_song_data = config['PATHS']['INPUT_SONG_DATA']
        output_data = config['PATHS']['OUTPUT_DATA']

        process_song_data(spark, input_song_data, output_data)    
        process_log_data(spark, input_log_data, output_data)
    finally:
        if spark:
            spark.stop()
    
if __name__ == "__main__":
    main()
