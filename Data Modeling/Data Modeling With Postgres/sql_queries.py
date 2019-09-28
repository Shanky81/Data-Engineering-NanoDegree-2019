# DROP TABLES as cleanup and reset database 
# List of tables used: (songplays, users, songs, artists, time)

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"


# CREATE TABLES (songplays, users, songs, artists, time)


user_table_create = ("""CREATE TABLE users(user_id VARCHAR NOT NULL, firstName VARCHAR(255) NOT NULL, lastName VARCHAR(255) NOT NULL, gender VARCHAR(1), level VARCHAR(50) NOT NULL,PRIMARY KEY (user_id))""")

song_table_create = ("""CREATE TABLE songs(song_id VARCHAR(100) NOT NULL, title VARCHAR(255) NOT NULL, artist_id VARCHAR(100), year INTEGER, duration DOUBLE PRECISION NOT NULL, 
PRIMARY KEY (song_id))""")

artist_table_create = ("""CREATE TABLE artists(artist_id VARCHAR(100) NOT NULL, name VARCHAR (255) NOT NULL, location VARCHAR(255), latitude DOUBLE PRECISION, longitude DOUBLE PRECISION,
PRIMARY KEY (artist_id))""")

time_table_create = ("""CREATE TABLE time(start_time timestamp NOT NULL, hour INTEGER, day INTEGER, week INTEGER, month INTEGER, year INTEGER, weekday INTEGER, \
PRIMARY KEY (start_time))""")

songplay_table_create = ("""CREATE TABLE songplays(songplay_id SERIAL NOT NULL, 
start_time TIMESTAMP REFERENCES time(start_time), 
user_id VARCHAR(50) REFERENCES users(user_id), 
level VARCHAR(50) NOT NULL, 
song_id VARCHAR(100) REFERENCES songs(song_id), 
artist_id VARCHAR(100) REFERENCES artists(artist_id), 
session_id BIGINT, 
location VARCHAR(255), 
user_agent TEXT, 
PRIMARY KEY (songplay_id))""")

# INSERT RECORDS

user_table_insert = ("""INSERT INTO users (user_id, firstName, lastName, gender, level) 
VALUES (%s, %s, %s, %s, %s) 
ON CONFLICT (user_id) DO UPDATE SET level=users.level """)

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude) 
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (artist_id) DO NOTHING """)

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday) 
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (start_time) DO NOTHING """)

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration) 
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (song_id) DO NOTHING """)

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
 VALUES (%s, %s, %s, %s, %s, %s, %s, %s) """)


# FIND SONGS

# FIND SONG BY ID
song_select = ("""SELECT COUNT(*) FROM songs s
WHERE s.song_id = %s
""")

# FIND ARTIST BY ID
artist_select = ("""SELECT COUNT(*) FROM artists a
WHERE a.artist_id = %s
""")

# FIND USER BY ID
user_select = ("""SELECT COUNT(*) FROM users u
WHERE u.user_id = %s
""")

# FIND TIME BY ID
time_select = ("""SELECT COUNT(*) FROM time t
WHERE t.start_time = %s
""")

# FIND SONGS BY USER_ID, SONG_ID AND ARTIST_ID
song_select_by_song_id_user_id_and_artist_id = ("""SELECT s.song_id, a.artist_id, a.user_id FROM songs s, artists a     
WHERE s.user_id = a.user_id AND s.artist_id = a.artist_id 
AND s.title = %s AND a.name = %s
AND s.duration = %s """)

# FIND SONGS BY SONG_ID AND ARTIST_ID
song_select_by_song_id_and_artist_id = ("""SELECT s.song_id, a.artist_id FROM songs s, artists a
WHERE s.artist_id = a.artist_id                                                                                                           AND s.title = %s                                                                                                                         AND a.name = %s                                                                                                                           AND s.duration = %s """)


# QUERY LISTS  

create_table_queries = [ time_table_create, user_table_create, song_table_create, artist_table_create, songplay_table_create]
  
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]