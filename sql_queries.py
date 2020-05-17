import configparser

# CONFIG

config = configparser.ConfigParser()
config.read('dwh.cfg')

# CREDENTIALS

ARN             = config.get('IAM_ROLE', 'ARN')
LOG_DATA        = config.get('S3', 'LOG_DATA')
LOG_JSONPATH    = config.get('S3', 'LOG_JSONPATH')
SONG_DATA       = config.get('S3', 'SONG_DATA')
ZONE            = config.get("AWS","ZONE")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events (
    artist VARCHAR,
    auth VARCHAR NOT NULL,
    firstName VARCHAR,
    gender VARCHAR,
    itemInSession INTEGER NOT NULL,
    lastName VARCHAR,
    length FLOAT,
    level VARCHAR NOT NULL,
    location VARCHAR,
    method VARCHAR NOT NULL,
    page VARCHAR NOT NULL,
    registration FLOAT,
    sessionId INTEGER NOT NULL,
    song VARCHAR,
    status INTEGER NOT NULL,
    ts numeric NOT NULL,
    userAgent VARCHAR,
    userId INTEGER
);""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs INTEGER NOT NULL,
    artist_id VARCHAR NOT NULL,
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location VARCHAR,
    artist_name VARCHAR NOT NULL,
    song_id VARCHAR NOT NULL,
    title VARCHAR NOT NULL,
    duration FLOAT NOT NULL,
    year INTEGER NOT NULL
);""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (
    songplay_id INTEGER IDENTITY, 
    start_time TIMESTAMP NOT NULL, 
    user_id VARCHAR NOT NULL, 
    level VARCHAR, 
    song_id VARCHAR, 
    artist_id VARCHAR, 
    session_id VARCHAR, 
    location VARCHAR, 
    user_agent VARCHAR,
    PRIMARY KEY (songplay_id),
    FOREIGN KEY (user_id) REFERENCES users (user_id),
    FOREIGN KEY (artist_id) REFERENCES artists (artist_id), 
    FOREIGN KEY (song_id) REFERENCES songs (song_id),
    FOREIGN KEY (start_time) REFERENCES time (start_time)
);""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (
    user_id VARCHAR, 
    first_name VARCHAR, 
    last_name VARCHAR, 
    gender VARCHAR,
    level VARCHAR,
    PRIMARY KEY (user_id)
);""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR, 
    title VARCHAR NOT NULL, 
    artist_id VARCHAR NOT NULL, 
    year INTEGER NOT NULL, 
    duration FLOAT NOT NULL, 
    PRIMARY KEY (song_id),
    FOREIGN KEY (artist_id) REFERENCES artists (artist_id)
);""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR, 
    name VARCHAR, 
    location VARCHAR, 
    latitude FLOAT, 
    longitude FLOAT, 
    PRIMARY KEY (artist_id)
);""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP, 
    hour INTEGER, 
    day INTEGER,
    week INTEGER,
    month INTEGER,
    year INTEGER,
    weekday VARCHAR,
    PRIMARY KEY (start_time)
);""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events FROM {} credentials 'aws_iam_role={}' format as json {} region '{}';
""").format(LOG_DATA, ARN, LOG_JSONPATH, ZONE)

staging_songs_copy = ("""
COPY staging_songs FROM {} credentials 'aws_iam_role={}' format as json 'auto' region '{}';
""").format(SONG_DATA, ARN, ZONE)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT timestamp 'epoch' + t0.ts / 1000 * interval '1 second' AS start_time, 
    t0.userId AS user_id,
    t0.level,
    t1.song_id,
    t1.artist_id,
    t0.sessionId AS session_id,
    t0.location,
    t0.userAgent AS user_agent
FROM staging_events AS t0
LEFT JOIN staging_songs AS t1 ON t0.song = t1.title AND t0.artist = t1.artist_name
WHERE t0.page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO users
SELECT t0.userId, t0.firstName, t0.lastName, t0.gender, t0.level
FROM staging_events t0
JOIN (
    SELECT max(ts) AS ts, userId
    FROM staging_events
    WHERE page = 'NextSong'
    GROUP BY userId
) t1 on t0.userId = t1.userId and t0.ts = t1.ts
""")

song_table_insert = ("""
INSERT INTO songs
SELECT song_id, title, artist_id, year, duration
FROM staging_songs
""")

artist_table_insert = ("""
INSERT INTO artists
SELECT DISTINCT artist_id, 
    artist_name AS name, 
    artist_location AS location, 
    artist_latitude AS latitude, 
    artist_longitude AS longitude
FROM staging_songs
""")

time_table_insert = ("""
INSERT INTO time
SELECT t0.start_time,
    EXTRACT(hour FROM t0.start_time) AS hour,
    EXTRACT(day FROM t0.start_time) AS day,
    EXTRACT(week FROM t0.start_time) AS week,
    EXTRACT(month FROM t0.start_time) AS month,
    EXTRACT(year FROM t0.start_time) AS year,
    EXTRACT(weekday FROM t0.start_time) AS weekday
FROM (
    SELECT distinct timestamp 'epoch' + ts / 1000 * interval '1 second' AS start_time
    FROM staging_events
    WHERE page = 'NextSong'
) t0
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
