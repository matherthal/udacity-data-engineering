import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS stg_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS stg_songs"
songplay_table_drop = "DROP TABLE IF EXISTS fact_song_plays;"
user_table_drop = "DROP TABLE IF EXISTS dim_user;"
song_table_drop = "DROP TABLE IF EXISTS dim_song;"
artist_table_drop = "DROP TABLE IF EXISTS dim_artist;"
time_table_drop = "DROP TABLE IF EXISTS dim_time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE stg_events (
    id BIGINT IDENTITY(1,1),
    num_songs INT NOT NULL,
    artist_id VARCHAR(100) NOT NULL,
    artist_latitude VARCHAR(100) NULL,
    artist_longitude VARCHAR(100) NULL,
    artist_location VARCHAR(100) NOT NULL,
    artist_name VARCHAR(100) NOT NULL,
    song_id VARCHAR(100) NOT NULL,
    title VARCHAR(100) NOT NULL,
    duration DECIMAL NOT NULL,
    year SMALLINT NOT NULL
);
""")

staging_songs_table_create = ("""
CREATE TABLE stg_songs (
    id BIGINT IDENTITY(1,1), 
    artist VARCHAR(100) NULL,
    auth VARCHAR(100) NOT NULL,
    firstName VARCHAR(100) NOT NULL,
    gender char(1) NOT NULL,
    itemInSession SMALLINT NOT NULL,
    lastName VARCHAR(100) NOT NULL,
    legth DECIMAL NULL,
    location VARCHAR(200) NOT NULL,
    method VARCHAR(4) NOT NULL,
    page VARCHAR(100) NOT NULL,
    registration BIGINT NOT NULL,
    sessionId INT NOT NULL,
    song VARCHAR(100) NULL,
    status SMALLINT NOT NULL,
    ts TIMESTAMP NOT NULL,
    userAgend VARCHAR(1000) NOT NULL,
    userId INT NOT NULL
);
""")

songplay_table_create = ("""
CREATE TABLE fact_song_plays (
    songplay_id INTEGER IDENTITY (1, 1),
    -- songplay_id VARCHAR(100) NOT NULL,
    user_id INT NOT NULL,
    song_id VARCHAR(100) NOT NULL,
    artist_id VARCHAR(100) NOT NULL,
    session_id INT NOT NULL,
    start_time TIMESTAMP NOT NULL, 
    level INT NOT NULL, 
    location VARCHAR(200) NOT NULL, 
    user_agent VARCHAR(1000) NOT NULL
)
DISTSTYLE KEY
DISTKEY (song_id)
SORTKEY (start_time);
""")

user_table_create = ("""
CREATE TABLE dim_user (
    -- id FLOAT DEFAULT nextval('dim_user_seq') NOT NULL,
    user_id INT, 
    first_name VARCHAR(100) NOT NULL, 
    last_name VARCHAR(100) NOT NULL, 
    gender CHAR(1) NOT NULL, 
    level INT NOT NULL
)
DISTSTYLE ALL;
""")

song_table_create = ("""
CREATE TABLE dim_song (
    -- id FLOAT DEFAULT nextval('dim_song_seq') NOT NULL,
    song_id VARCHAR(100), 
    title VARCHAR(100) NOT NULL, 
    artist_id VARCHAR(100) NOT NULL, 
    year SMALLINT NOT NULL, 
    duration DECIMAL NOT NULL
)
DISTSTYLE KEY
DISTKEY (song_id);
""")

artist_table_create = ("""
CREATE TABLE dim_artist (
    -- id FLOAT DEFAULT nextval('dim_artist_seq') NOT NULL,
    artist_id VARCHAR(100), 
    name VARCHAR(100) NOT NULL, 
    location VARCHAR(200) NOT NULL, 
    lattitude FLOAT NULL, 
    longitude FLOAT NULL
)
DISTSTYLE ALL;
""")

time_table_create = ("""
CREATE TABLE dim_time (
    -- id FLOAT DEFAULT nextval('dim_time_seq') NOT NULL,
    start_time TIMESTAMP, 
    hour SMALLINT NOT NULL, 
    day SMALLINT NOT NULL, 
    week SMALLINT NOT NULL, 
    month SMALLINT NOT NULL, 
    year SMALLINT NOT NULL, 
    weekday SMALLINT NOT NULL
)
DISTSTYLE ALL
SORTKEY (start_time);
""")

# STAGING TABLES

# Invalid credentials. Must be of the format: credentials 'aws_iam_role=...' or 'aws_access_key_id=...;aws_secret_access_key=...[;token=...]'
staging_events_copy = ("""
COPY stg_events 
FROM '{}'
CREDENTIALS 'aws_iam_role={}'
REGION 'us-west-2'
FORMAT AS json '{}';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE'], config['S3']['LOG_JSONPATH'])


# -- copy stg_events from 's3://udacity-dend/song_data' 
# -- credentials 'aws_iam_role={}' 
# -- gzip delimiter ';' compupdate off region 'us-west-2';

staging_songs_copy = ("""
COPY stg_songs 
FROM '{}' 
CREDENTIALS 'aws_iam_role={}' 
REGION 'us-west-2';
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE'])

# FINAL TABLES

songplay_table_insert = ("""

""")

user_table_insert = ("""

""")

song_table_insert = ("""

""")

artist_table_insert = ("""
INSERT INTO dim_artist (artist_id, name, location, lattitude, longitude)
SELECT DISTINCT artist_id, artist_latitude, artist_longitude, artist_location, artist_name
FROM stg_events;
""")

time_table_insert = ("""

""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
