import configparser
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

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
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select([
        'song_id', 'title', 'artist_id', 'year','duration']).drop_duplicates()

    # write songs table to parquet files partitioned by year and artist
    songs_table.coalesce(1).write.mode('overwrite')\
        .partitionBy("year", "artist_id")\
        .parquet(os.path.join(output_data, 'song'))

    # extract columns to create artists table
    artists_table = df.select([
        'artist_id', 'artist_name', 'artist_location', 'artist_latitude', 
        'artist_longitude'
    ]).drop_duplicates()

    # write artists table to parquet files
    artists_table.coalesce(1).write.mode('overwrite')\
        .parquet(os.path.join(output_data, 'artist'))


def process_log_data(spark, input_data, output_data):
    """Process user log data creating the tables user, time and songplays

    Args:
        spark (SparkSession): The spark session object
        input_data (str): The input files path
        output_data (str): The output files path
    """
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data/*/*/*.json')

    # read log data file
    log_df = spark.read.json(log_data)

    # filter by actions for song plays
    log_df = log_df.where(F.col('page')=='NextSong')

    # extract columns for users table    
    user_table = log_df.select([
        'userId', 'firstName', 'lastName', 'gender', 'level'])

    # write users table to parquet files
    user_path = os.path.join(output_data, 'user')
    user_table.coalesce(1).write.mode('overwrite').parquet(user_path)

    # create timestamp column from original timestamp column
    get_timestamp = F.udf(lambda x: x/1000)
    log_df = log_df.withColumn('ts_ms', get_timestamp('ts'))

    # create datetime column from original timestamp column
    # get_datetime = udf()
    # df = df.withColumn('dt', F.from_unixtime((F.col('ts')/1000)))
    log_df = log_df.withColumn('dt', F.from_unixtime(F.col('ts_ms')))

    # extract columns to create time table
    time_table = log_df.select(
        'ts', 
        F.hour('dt').alias('hour'), 
        F.dayofmonth('dt').alias('day'), 
        F.weekofyear('dt').alias('weekofyear'),
        F.month('dt').alias('month'),
        F.year('dt').alias('year'),
        F.dayofweek('dt').alias('weekday')
    )

    # write time table to parquet files partitioned by year and month
    time_table.coalesce(1).write.mode('overwrite')\
        .partitionBy('year', 'month')\
        .parquet(os.path.join(output_data, 'time'))

    # read in song data to use for songplays table
    song_df = spark.read.parquet(os.path.join(output_data, 'song'))
    artist_df = spark.read.parquet(os.path.join(output_data, 'artist'))

    song_df = artist_df.select(['artist_name', 'artist_id'])\
        .join(song_df, on='artist_id', how='inner')

    # extract columns from joined song and log datasets to create songplays 
    on_clause = \
        (song_df.title == log_df.song) \
        & (song_df.artist_name == log_df.artist) \
        & (song_df.duration == log_df.length)
    songplays_table = log_df.join(song_df, on_clause, how='inner')

    songplays_table = songplays_table.selectExpr(
        'ts as start_time', 'userId as user_id', 'level', 'song_id', 
        'artist_id', 'itemInSession as session_id', 'location', 
        'userAgent as user_agent')

    key_columns = [
        'start_time', 'user_id', 'song_id', 'artist_id', 'session_id']
    song_plays = song_plays.withColumn(
        'songplay_id', 
        F.sha2(F.concat_ws("||", *key_columns), 256)
    )

    # write songplays table to parquet files partitioned by year and month
    songplays_table.coalesce(1).write.mode('overwrite')\
        .partitionBy('year', 'month')\
        .parquet(os.path.join(output_data, 'songplays'))

def main():
    spark = create_spark_session()
    try:
        input_data = "s3a://udacity-dend/"
        output_data = "s3a://udacity-study/datalake-table/"

        process_song_data(spark, input_data, output_data)    
        process_log_data(spark, input_data, output_data)
    finally:
        if spark:
            spark.stop()
    
if __name__ == "__main__":
    main()