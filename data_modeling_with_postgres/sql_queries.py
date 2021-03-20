# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS rtists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

songplay_table_create = ("""
-- - records in log data associated with song plays i.e. records with page NextSong
CREATE TABLE IF NOT EXISTS songplays (
    id SERIAL PRIMARY KEY, 
    start_time BIGINT NOT NULL, 
    user_id INT NOT NULL, 
    level VARCHAR NOT NULL, 
    song_id VARCHAR, 
    artist_id VARCHAR, 
    session_id INT NOT NULL, 
    location VARCHAR NOT NULL, 
    user_agent VARCHAR NOT NULL,
    CONSTRAINT unique_user_song_time UNIQUE (start_time, user_id, song_id)
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY, 
    user_id INT NOT NULL UNIQUE, 
    first_name VARCHAR NOT NULL, 
    last_name VARCHAR NOT NULL, 
    gender CHAR NOT NULL, 
    level VARCHAR NOT NULL
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    id SERIAL PRIMARY KEY, 
    song_id VARCHAR NOT NULL UNIQUE, 
    title VARCHAR NOT NULL, 
    artist_id VARCHAR NOT NULL, 
    year INT NOT NULL, 
    duration DECIMAL NOT NULL
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    id SERIAL PRIMARY KEY, 
    artist_id VARCHAR NOT NULL UNIQUE, 
    name VARCHAR NOT NULL, 
    location VARCHAR NOT NULL, 
    latitude NUMERIC NOT NULL, 
    longitude NUMERIC NOT NULL
);
""")

time_table_create = ("""
-- timestamps of records in songplays broken down into specific units
CREATE TABLE IF NOT EXISTS time (
    id SERIAL PRIMARY KEY, 
    start_time BIGINT NOT NULL UNIQUE, 
    hour INT NOT NULL, 
    day INT NOT NULL, 
    week INT NOT NULL, 
    month INT NOT NULL, 
    year INT NOT NULL, 
    weekday INT NOT NULL
);
""")

# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO songplays (
    start_time, user_id, level, song_id, artist_id, session_id, location, 
    user_agent) 
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (start_time, user_id, song_id) DO UPDATE SET level=songplays.level;
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level) 
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (user_id) DO UPDATE SET level=users.level;
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration) 
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (song_id) DO NOTHING;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude) 
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (artist_id) DO NOTHING;
""")


time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (start_time) DO NOTHING;
""")

# FIND SONGS
song_select = ("""
SELECT s.song_id, s.artist_id
FROM songs s
JOIN artists a ON a.artist_id = s.artist_id
WHERE s.title = %s AND a.name = %s AND duration = %s;
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]