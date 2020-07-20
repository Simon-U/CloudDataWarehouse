import configparser


# CONFIG
config = configparser.ConfigParser()
config.read(['dwh.cfg', 'condwh.cfg'])

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay_table"
user_table_drop = "DROP TABLE IF EXISTS user_table"
song_table_drop = "DROP TABLE IF EXISTS song_table"
artist_table_drop = "DROP TABLE IF EXISTS artist_table"
time_table_drop = "DROP TABLE IF EXISTS time_table"

# CREATE TABLES

staging_songs_table_create= ("""CREATE TABLE IF NOT EXISTS staging_songs (
num_songs int,
artist_id varchar,
artist_latitude numeric,
artist_longitude numeric,
artist_location varchar(max),
artist_name varchar(max),
song_id varchar,
title varchar(max),
duration numeric,
year int)
diststyle even;""")

staging_events_table_create = ("""CREATE TABLE IF NOT EXISTS staging_events (
artist varchar(255) encode text255, 
auth varchar(255) encode text255, 
firstName varchar(100),
gender varchar(1), 
itemInSession integer,
lastName varchar(100),
length DOUBLE PRECISION,
level varchar(20),
location varchar(255) encode text255,
method varchar(10),
page varchar(50),
registration varchar(100),
sessionId integer,
song varchar(200),
status integer,
ts bigint,
userAgent varchar(255) encode text255,
userId integer)
diststyle even;""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplay_table (
song_play_id bigint IDENTITY(0,1) PRIMARY KEY,
start_time varchar REFERENCES time_table(start_time) SORTKEY,
user_id int REFERENCES user_table(user_id),
song_id varchar REFERENCES song_table(song_id) DISTKEY,
artist_id varchar REFERENCES artist_table(artist_id),
session_id varchar NOT NULL,
user_agent varchar NOT NULL,
location varchar NOT NULL)
;""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS user_table (
user_id int PRIMARY KEY SORTKEY,
first_name varchar NOT NULL,
last_name varchar NOT NULL,
gender varchar NOT NULL,
level varchar NOT NULL)
diststyle auto;
;""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS song_table (
song_id varchar PRIMARY KEY SORTKEY DISTKEY,
title varchar NOT NULL,
artist_id varchar REFERENCES artist_table(artist_id),
year int,
duration float NOT NULL);
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artist_table (
artist_id varchar PRIMARY KEY SORTKEY,
name varchar NOT NULL,
location varchar,
latitude varchar,
longitude varchar)
diststyle all;
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time_table (
start_time varchar PRIMARY KEY SORTKEY,
hour int,
day int, 
week int,
month int,
year int,
weekday int)
diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""copy staging_events from {}
    credentials 'aws_iam_role={}'
    JSON {}
    region 'us-west-2';
""").format(config.get('S3', 'LOG_DATA'), config.get('DWH', 'DWH_ROLE_ARN'), config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""copy staging_songs from {} 
    credentials 'aws_iam_role={}'
    JSON 'auto'
    region 'us-west-2';
""").format(config.get('S3', 'SONG_DATA'), config.get('DWH', 'DWH_ROLE_ARN'))

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplay_table (start_time, user_id, song_id, artist_id, session_id, user_agent, location)
SELECT   events.ts,
         events.userId,
         songs.song_id,
         songs.artist_id,
         events.sessionId,
         events.userAgent,
         events.location
FROM staging_events AS events
JOIN staging_songs AS songs
     ON (events.artist = songs.artist_name)
     WHERE events.page = 'NextSong';
""")
 
user_table_insert = ("""INSERT INTO user_table (user_id, first_name, last_name, gender, level)
SELECT DISTINCT userId, firstName, lastName, gender, level
FROM staging_events
WHERE page='NextSong'
""")

song_table_insert = ("""INSERT INTO song_table (song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id, title, artist_id, year, duration
FROM staging_songs
""")

artist_table_insert = ("""INSERT INTO artist_table (artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM staging_songs
""")

time_table_insert = ("""INSERT INTO time_table (start_time, hour, day, week, month, year, weekday) 
SELECT a.start_time, 
EXTRACT (HOUR FROM a.start_time), EXTRACT (DAY FROM a.start_time), 
EXTRACT (WEEK FROM a.start_time), EXTRACT (MONTH FROM a.start_time),
EXTRACT (YEAR FROM a.start_time), EXTRACT (WEEKDAY FROM a.start_time) FROM 
(SELECT TIMESTAMP 'epoch' + start_time/1000 *INTERVAL '1 second' as start_time FROM songplay_table) a;
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, artist_table_create, song_table_create,  time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, artist_table_insert, song_table_insert, songplay_table_insert, time_table_insert]