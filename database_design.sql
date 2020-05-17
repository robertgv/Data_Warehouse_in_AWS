CREATE TABLE IF NOT EXISTS staging_events (
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
);

CREATE TABLE IF NOT EXISTS staging_songs (
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
);

CREATE TABLE IF NOT EXISTS songplays ( 
    songplays_id INTEGER,
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
);

CREATE TABLE IF NOT EXISTS users (
    user_id VARCHAR, 
    first_name VARCHAR, 
    last_name VARCHAR, 
    gender VARCHAR,
    level VARCHAR,
    PRIMARY KEY (user_id)
);

CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR, 
    title VARCHAR NOT NULL, 
    artist_id VARCHAR NOT NULL, 
    year INTEGER NOT NULL, 
    duration FLOAT NOT NULL, 
    PRIMARY KEY (song_id),
    FOREIGN KEY (artist_id) REFERENCES artists (artist_id)
);

CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR, 
    name VARCHAR, 
    location VARCHAR, 
    latitude FLOAT, 
    longitude FLOAT, 
    PRIMARY KEY (artist_id)
);

CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP, 
    hour INTEGER, 
    day INTEGER,
    week INTEGER,
    month INTEGER,
    year INTEGER,
    weekday VARCHAR,
    PRIMARY KEY (start_time)
);
