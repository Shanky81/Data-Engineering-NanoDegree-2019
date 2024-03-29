import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    '''
    Args:
        cur: cursor connection
        filepath: Song file path from /data/song_data directory.
        
    Returns: 
        data is inserted into Song and artist table.
    '''
    
    # open song file
    df = pd.read_json(filepath, typ='series')

    # insert song record
    song_data = df[['song_id','title','artist_id', 'year', 'duration']]
    
    # song_id duplicate check
    cur.execute(song_select, (song_data[['song_id']]))
    results = cur.fetchone()
    
    # if 0 then no duplication present and can insert records
    if results[0] == 0:
        cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id','artist_name','artist_location', 'artist_latitude', 'artist_longitude']]
    
    # artist_id duplicate check
    cur.execute(artist_select, (artist_data[['artist_id']]))
    results = cur.fetchone()
   
    # if 0 then no duplication present and can insert records
    if results[0] == 0:
        cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    
    '''
    Args:
        cur: cursor connection
        filepath: log file path from /data/log_data directory.
    
    Returns: 
        data is inserted into time, user and songplays table.
    '''
    
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    #"t =" not valid 
    df['ts'] = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = [df['ts'], df['ts'].dt.hour, df['ts'].dt.day,
                 df['ts'].dt.weekofyear, df['ts'].dt.month,
                 df['ts'].dt.year,df['ts'].dt.weekday]
    column_labels = ['ts', 'hour', 'day', 'week of year', 'month', 'year', 'weekday']
    
    assert isinstance(time_data, list), 'time_data should have been a list'
    assert isinstance(column_labels, list), 'column_labels should have been a list'

    dictionary = dict(zip(column_labels, time_data))
    time_df = pd.DataFrame.from_dict(dictionary)

    assert isinstance(time_df, pd.DataFrame), 'time_df should have been a dataframe'
    
    for i, row in time_df.iterrows():
        # check for start_time duplicates
        cur.execute(time_select, [row.ts])
        results = cur.fetchone()
        
        # if 0 then no duplication present and can insert records
        if results[0] == 0: 
            cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        # check for user_id duplicates
        cur.execute(user_select, (str(row.userId),))
        results = cur.fetchone()
        
        # if 0 then no duplication present and can insert records
        if results[0] == 0:
            cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select_by_song_id_and_artist_id, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    
    '''
    Args:
        cur: cursor (temp memory) to process data row by row from set of rows with current           row pointer.
        conn: db connection to keep connectivity while DML.
        filepath: log file path from /data/log_data directory.
        func: paramerter to call function and process the data.
        
    Returns: 
        By calling function, data is inserted into time, user and songplays table.
    '''
    
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
    
    # establish connection with DB
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
    
#restart kernel