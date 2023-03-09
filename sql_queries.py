"""
This script contains the collection of queries for creating the schemas, load the data and perform testing
and analytics.
"""

import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

DWH_ROLE_ARN = config.get('IAM_ROLE', 'DWH_ROLE_ARN')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS events_staging;"
staging_songs_table_drop = "DROP TABLE IF EXISTS songs_staging;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create = ("""
    CREATE TABLE "events_staging" (
    "artist"         character varying(200)    sortkey,
    "auth"           character varying(50),
    "firstName"      character varying(20),
    "gender"         character varying(10),
    "itemInSession"  smallint,
    "lastName"       character varying(20),
    "length"         decimal(9,5),
    "level"          character varying(20),
    "location"       character varying(100),
    "method"         character varying(10),
    "page"           character varying(50),
    "registration"   decimal(14,1),
    "sessionId"      smallint,
    "song"           character varying(200),
    "status"         smallint,
    "ts"             bigint,
    "userAgent"      character varying(300),
    "userId"         character varying(10)     distkey
);
""")

staging_songs_table_create = ("""
CREATE TABLE "songs_staging" (
    "artist_id"        character varying(50)   distkey,
    "artist_latitude"  decimal(9,6),
    "artist_location"  character varying(200),
    "artist_longitude" decimal(9,6),
    "artist_name"      character varying(200)  sortkey,
    "duration"         decimal(9,5),
    "num_songs"        smallint,
    "song_id"          character varying(50),
    "title"            character varying(200),
    "year"             smallint
);
""")

songplay_table_create = ("""
CREATE TABLE "songplays" (
    "songplay_id" INT IDENTITY(0,1),
    "start_time"  TIMESTAMPTZ             NOT NULL,
    "user_id"     character varying(10)   NOT NULL,
    "level"       character varying(20),
    "song_id"     character varying(50)   NOT NULL,
    "artist_id"   character varying(50)   NOT NULL,
    "session_id"  smallint                NOT NULL,
    "location"    character varying(100),
    "user_agent"  character varying(300)
);
""")

user_table_create = ("""
CREATE TABLE "users" (
  "user_id"      character varying(10)    not null,
  "first_name"   character varying(20)    not null,
  "last_name"    character varying(20),
  "gender"       character varying(10),
  "level"        character varying(20))
diststyle all;
""")

song_table_create = ("""
CREATE TABLE "songs" (
  "song_id"      character varying(50)    sortkey,
  "title"        character varying(200),
  "artist_id"    character varying(50),   
  "year"         smallint,
  "duration"     decimal(9,5))
diststyle all;
""")

artist_table_create = ("""
CREATE TABLE "artists" (
  "artist_id"    character varying(50)    not null   sortkey,
  "name"         character varying(200),
  "location"     character varying(200),   
  "latitude"    decimal(9,6),
  "longitude"    decimal(9,6))
diststyle all;
""")

time_table_create = ("""
CREATE TABLE "time" (
  "start_time"   TIMESTAMPTZ,
  "hour"         smallint,
  "day"          smallint,   
  "week"         smallint,
  "month"        smallint,
  "year"         smallint,
  "weekday"      boolean)
diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""copy events_staging from 's3://udacity-dend/log_data'
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    json 's3://udacity-dend/log_json_path.json';
""").format(DWH_ROLE_ARN)

staging_songs_copy = ("""copy songs_staging from 's3://udacity-dend/song_data'
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    format as json 'auto';
""").format(DWH_ROLE_ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, 
                       user_agent)
SELECT to_timestamp((timestamp 'epoch' + e.ts/1000 * interval '1 second'), 'YYYY-MM-DD HH24:MI:SS') AS start_time,
       e.userid       AS user_id,
       e.level,
       s.song_id,
       s.artist_id,
       e.sessionid    AS session_id,
       e.location,
       e.useragent    AS user_agent
FROM events_staging e
JOIN songs_staging s  ON (e.song = s.title);
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT distinct userid     AS user_id,
       firstname           AS first_name,
       lastname            AS last_name,
       gender,
       level
FROM events_staging
WHERE firstname IS NOT NULL;
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT distinct song_id,
       title,
       artist_id,
       year,
       duration
FROM songs_staging;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT distinct artist_id,
       artist_name         AS name,
       artist_location     AS location,
       artist_latitude     AS latitude,
       artist_longitude    AS longitude
FROM songs_staging;
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
WITH ts_conv as (
  select distinct(start_time) AS timest
  from songplays
)
SELECT ts.timest,
       EXTRACT(hour from ts.timest) as hour,
       EXTRACT(day from ts.timest) as day,
       EXTRACT(week from ts.timest) as week,
       EXTRACT(month from ts.timest) as month,
       EXTRACT(year from ts.timest) as year,
       CASE WHEN EXTRACT(dayofweek FROM ts.timest) IN (2, 3, 4, 5, 6) THEN true ELSE false END AS weekday
FROM ts_conv ts;
""")

# ANALYTICAL QUERY
# NUMBER OF ROWS

staging_events_rows = ("""
SELECT count(*) FROM events_staging;
""")

staging_songs_rows = ("""
SELECT count(*) FROM songs_staging;
""")

songplays_rows = ("""
SELECT count(*) FROM songplays;
""")

users_rows = ("""
SELECT count(*) FROM users;
""")

songs_rows = ("""
SELECT count(*) FROM songs;
""")

artists_rows = ("""
SELECT count(*) FROM artists;
""")

time_rows = ("""
SELECT count(*) FROM time;
""")

# SELECT QUERIES

popular_artist_gender = ("""
SELECT artists.name, time.year, count(*)as reproductions
FROM songplays
JOIN artists        on (artists.artist_id = songplays.artist_id)
JOIN time           on (time.start_time = songplays.start_time)
JOIN users          on (users.user_id = songplays.user_id)
WHERE users.gender = 'F'
GROUP BY (artists.name, time.year)
order by count(*) desc
LIMIT 3;
""")

popular_song_location = ("""
SELECT songs.title, songplays.location, count(*) as reproductions
FROM  songplays
JOIN  songs        on (songs.song_id = songplays.song_id)
WHERE songplays.location = 'Atlanta-Sandy Springs-Roswell, GA'
GROUP BY (songs.title, songplays.location)
order by count(*) desc
LIMIT 1;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
row_table_queries = [staging_events_rows, staging_songs_rows, songplays_rows, songs_rows, users_rows, artists_rows, time_rows]
analytic_table_queries = [popular_artist_gender, popular_song_location]
