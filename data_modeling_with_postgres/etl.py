import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Process a single song file persisting songs and artists
    
    Args:
        cur (cursor): a cursor to sparkfydb
        filepath (str): the file path to the json song file
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    df['year'] = df['year'].astype(int)
    df['duration'] = df['duration'].astype(float)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year','duration']].values
    for row in song_data:
        cur.execute(song_table_insert, list(row))
    
    # insert artist record
    artist_data = df[[
        'artist_id', 'artist_name', 'artist_location', 'artist_latitude', 
        'artist_longitude'
    ]].values
    for row in artist_data:
        cur.execute(artist_table_insert, list(row))


def process_log_file(cur, filepath):
    """
    Process a single log file persisting the songplay facts and the time dimension
    
    The songplay records keep references to song_id and artist_id, however if they are
    not found, they will be persisted as None.
    
    Args:
        cur (cursor): a cursor to sparkfydb
        filepath (str): the file path to the json log file
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page']=='NextSong']

    # convert timestamp column to datetime
    t = df[['ts']]
    t['dt'] = pd.to_datetime(t['ts'], unit='ms')
    t['hour'] = t['dt'].dt.hour
    t['day'] = t['dt'].dt.day
    t['weekofyear'] = t['dt'].dt.weekofyear
    t['month'] = t['dt'].dt.month
    t['year'] = t['dt'].dt.year
    t['weekday'] = t['dt'].dt.weekday
    
    # insert time data records
    time_data = t[[
        'ts', 'hour', 'day', 'weekofyear', 'month', 'year', 'weekday'
    ]].values.tolist()
    column_labels = ['ts', 'hour', 'day', 'weekofyear', 'month', 'year', 'weekday']
    time_df = pd.DataFrame(time_data, columns=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (
            row.ts, 
            row.userId, 
            row.level, 
            songid, 
            artistid, 
            row.sessionId, 
            row.location, 
            row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Find json files recursively in the filepath and call the function `func` passing the
    cur and the file found, for each file.
    
    Args:
        cur (cursor): a cursor to sparkfydb
        conn (connection): the connection to sparkfydb
        filepath (str): a directory where to find the files
        func (function): a function to be called for the files found
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    Persist song plays facts and the dimensions user, song, artist and time
    in a star schema fashion.
    
    - Create a connection to sparkfydb
    
    - For each song file persist the song and artist data
    
    - For each user log file persist the song plays, as well as the users and the time
    
    - Close the connection
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()