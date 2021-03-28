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

staging_songs_table_create= ("""
CREATE TABLE stg_songs (
    id BIGINT IDENTITY(1,1),
    num_songs INT,
    artist_id TEXT,
    artist_latitude TEXT,
    artist_longitude TEXT,
    artist_location TEXT,
    artist_name TEXT,
    song_id TEXT,
    title TEXT,
    duration DECIMAL,
    year SMALLINT
);
""")

staging_events_table_create = ("""
CREATE TABLE stg_events (
    artist TEXT,
    auth VARCHAR(100),
    firstName VARCHAR(100),
    gender char(1),
    itemInSession SMALLINT,
    lastName VARCHAR(100),
    length DECIMAL,
    level VARCHAR(100),
    location TEXT,
    method VARCHAR(4),
    page VARCHAR(100),
    registration FLOAT,
    sessionId INT,
    song TEXT,
    status SMALLINT,
    ts BIGINT,
    userAgent TEXT,
    userId INT
);
""")

songplay_table_create = ("""
CREATE TABLE fact_song_plays (
    id INTEGER IDENTITY (1, 1),
    -- songplay_id VARCHAR(100) NOT NULL,
    user_id INT NOT NULL,
    song_id VARCHAR(100) NOT NULL,
    artist_id VARCHAR(100) NOT NULL,
    session_id INT NOT NULL,
    start_time TIMESTAMP NOT NULL, 
    level VARCHAR(100) NOT NULL, 
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
    first_name VARCHAR(100) NULL, 
    last_name VARCHAR(100) NULL, 
    gender CHAR(1) NULL, 
    level VARCHAR(100) NOT NULL
)
DISTSTYLE ALL;
""")

song_table_create = ("""
CREATE TABLE dim_song (
    -- id FLOAT DEFAULT nextval('dim_song_seq') NOT NULL,
    song_id TEXT, 
    title TEXT NOT NULL, 
    artist_id TEXT NOT NULL, 
    year SMALLINT NOT NULL, 
    duration DECIMAL NOT NULL
)
DISTSTYLE KEY
DISTKEY (song_id);
""")

artist_table_create = ("""
CREATE TABLE dim_artist (
    -- id FLOAT DEFAULT nextval('dim_artist_seq') NOT NULL,
    artist_id TEXT, 
    name TEXT NULL, 
    location TEXT NULL, 
    latitude FLOAT NULL, 
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
staging_songs_copy = ("""
COPY stg_songs 
FROM '{}'
CREDENTIALS 'aws_iam_role={}'
COMPUPDATE OFF 
REGION 'us-west-2'
FORMAT JSON 'auto';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])


# -- copy stg_events from 's3://udacity-dend/song_data' 
# -- credentials 'aws_iam_role={}' 
# -- gzip delimiter ';' compupdate off region 'us-west-2';

staging_events_copy = ("""
COPY stg_events 
FROM '{}' 
CREDENTIALS 'aws_iam_role={}' 
COMPUPDATE OFF 
REGION 'us-west-2'
FORMAT AS json '{}';
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO fact_song_plays(
    user_id, song_id, artist_id, session_id, start_time, level, location, user_agent)
SELECT 
	userId AS user_id, song_id, artist_id, sessionId AS session_id, 
    TIMESTAMP 'EPOCH' + (ts/1000) * interval '1 second' AS start_time, 
    level, location, useragent AS user_agent
FROM stg_events evt
JOIN stg_songs sng 
	ON LOWER(evt.artist)=LOWER(sng.artist_name) AND LOWER(evt.song)=LOWER(sng.title)
;
""")

user_table_insert = ("""
INSERT INTO dim_user(user_id, first_name, last_name, gender, level)
SELECT userId, firstName, lastName, gender, level
FROM stg_events;
""")

song_table_insert = ("""
INSERT INTO dim_song (song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id, title, artist_id, year, duration
FROM stg_songs;
""")

artist_table_insert = ("""
INSERT INTO dim_artist (artist_id, name, location, latitude, longitude)
SELECT DISTINCT 
	artist_id, 
    artist_name,
    artist_location,
    CAST (artist_latitude AS FLOAT),
	CAST (artist_longitude AS FLOAT)
FROM stg_songs;
""")

time_table_insert = ("""
INSERT INTO dim_time(start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT
    t.start_time,
	EXTRACT(HOUR FROM t.start_time) AS hour, 
    EXTRACT(DAY FROM t.start_time) AS day, 
	EXTRACT(WEEK FROM t.start_time) AS week, 
    EXTRACT(MONTH FROM t.start_time) AS month, 
    EXTRACT(YEAR FROM t.start_time) AS year, 
    EXTRACT(WEEKDAY FROM t.start_time) AS weekday
FROM (
    SELECT TIMESTAMP 'EPOCH' + (ts/1000) * interval '1 second' AS start_time
    FROM stg_events
) AS t
;
""")

# QUERY LISTS

create_table_queries = {
    'staging_events_table_create': staging_events_table_create, 
    'staging_songs_table_create': staging_songs_table_create, 
    'songplay_table_create': songplay_table_create, 
    'user_table_create': user_table_create, 
    'song_table_create': song_table_create, 
    'artist_table_create': artist_table_create, 
    'time_table_create': time_table_create
}
drop_table_queries = {
    'staging_events_table_drop': staging_events_table_drop, 
    'staging_songs_table_drop': staging_songs_table_drop, 
    'songplay_table_drop': songplay_table_drop, 
    'user_table_drop': user_table_drop, 
    'song_table_drop': song_table_drop, 
    'artist_table_drop': artist_table_drop, 
    'time_table_drop': time_table_drop
}
copy_table_queries = {
    'staging_events_copy': staging_events_copy, 
    'staging_songs_copy': staging_songs_copy
}
insert_table_queries = {
    'songplay_table_insert': songplay_table_insert, 
    'user_table_insert': user_table_insert, 
    'song_table_insert': song_table_insert, 
    'artist_table_insert': artist_table_insert, 
    'time_table_insert': time_table_insert
}
