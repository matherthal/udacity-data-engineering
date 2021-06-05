# Sparkfy  

This project creates a star schema optimized database for queries and analysis on song plays.

## Getting Started

This project was tested in a EMR cluster with the following configuration:

- emr-5.28.0
- instance-count 3
- instance-type m5.xlarge
- applications Spark, Zeppelin, Livy, Hadoop, Hive

Also it uses the jar package:

- org.apache.hadoop:hadoop-aws:2.7.3 that can be found [here](https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-aws/2.7.3).

After creating the cluster, it's necessary to copy the following files to the home folder in the master node:

- `etl.py`
- `dl.cfg`

This can be done using the s3, or with a scp command, such as:

```
scp -i <my key>.pem etl.py dl.cfg hadoop@<master node url>:
```

Then the script can be executed with:

```
spark-submit etl.py
```

After processing, the data will be writen in `s3://udacity-study/datalake-table/` as defined in the `etl.py` script.

## Data Source

The songs and artists data come from the [Million Song Dataset](http://millionsongdataset.com/), which is a free collection of music metadata. And for testing we have generated fake song play logs using the [event simulator](https://github.com/Interana/eventsim). 

Both these data sources are in the S3 bucket `s3://udacity-dend/`.

## Data Model

To allow fast queries we have created a star schema for the song plays. 

The model consists of one fact table called songplays and its dimensions, namely, the song, artist, user and time.


- Fact Table
    - songplays - records in log data associated with song plays i.e. records with page NextSong
        - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
- Dimension Tables
    - users - users in the app
        - user_id, first_name, last_name, gender, level
    - songs - songs in music database
        - song_id, title, artist_id, year, duration
    - artists - artists in music database
        - artist_id, name, location, lattitude, longitude
    - time - timestamps of records in songplays broken down into specific units
        - start_time, hour, day, week, month, year, weekday

### Example of queries

Loading the song_table:

```python
import os

output_data = 's3://udacity-study/datalake-table/'

song_table = spark.parquet.read(os.path.join(output_data, "song/"))
song_table.createOrReplaceTempView("song_table")
```

Obtaining some songs:

```sql
spark.sql('''
select * from song_table limit 3
''').show()
```

Loading song plays table:

```python
songplays_table.read.parquet(os.path.join(output_data, 'songplays'))
songplays_table.createOrReplaceTempView("songplays_table")
```

Obtaining some song plays:

```sql
spark.sql('''
select * 
from songplays_table spt 
join song_table st on spt.song_id=st.song_id
limit 10
''').show()
```

## ETL

This project contains 2 important files:

- `etl.py`
    - Contains all the business rules to process the song and log files into parquet tables
- `dl.cfg`
    - Contains the configurations necessary to the execution

The ETL for this project has 2 steps:

1. `process_song_data` 
    - Processing the songs and artist data from the *Million Song Dataset*
    - Writing parquet tables for the dimensions songs and artist
2. `process_log_data` 
    - Processing user data logs from the generated logs
    - Creating the parquet tables for user and time dimensions, as well as for the song plays facts
