import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create = ("""CREATE TABLE staging_events (event_id        int      IDENTITY(0,1) PRIMARY KEY,
                                                               artist          varchar,
                                                               auth            varchar,
                                                               first_name      varchar,
                                                               gender          char,
                                                               item_in_session int,
                                                               last_name       varchar,
                                                               length          numeric,
                                                               level           varchar,
                                                               location        varchar,
                                                               method          varchar,
                                                               page            varchar,
                                                               registration    bigint,
                                                               session_id      int,
                                                               song            varchar,
                                                               status          int,
                                                               ts              bigint,
                                                               user_agent      varchar,
                                                               user_id         int
                                                              );""")

staging_songs_table_create = ("""CREATE TABLE staging_songs (num_songs         int,
                                                             artist_id         varchar,
                                                             artist_latitude   numeric,
                                                             artist_longtitude numeric,
                                                             artist_location   varchar,
                                                             artist_name       varchar,
                                                             song_id           varchar,
                                                             title             varchar,
                                                             duration          numeric,
                                                             year              int
                                                            );""")

songplay_table_create = ("""CREATE TABLE songplays (songplay_id int       IDENTITY(0,1) PRIMARY KEY,
                                                    start_time  timestamp NOT NULL,
                                                    user_id     int       NOT NULL  distkey,
                                                    level       varchar,
                                                    song_id     varchar,
                                                    artist_id   varchar,
                                                    session_id  int,
                                                    location    varchar,
                                                    user_agent  varchar
                                                   );""")

user_table_create = ("""CREATE TABLE users (user_id    int distkey PRIMARY KEY,
                                            first_name varchar,
                                            last_name  varchar,
                                            gender     char,
                                            level      varchar
                                           );""")

song_table_create = ("""CREATE TABLE songs (song_id   varchar  PRIMARY KEY,
                                            title     varchar,
                                            artist_id varchar,
                                            year      int,
                                            duration  numeric
                                           );""")

artist_table_create = ("""CREATE TABLE artists (artist_id varchar  PRIMARY KEY,
                                                name      varchar,
                                                location  varchar,
                                                latitude  numeric,
                                                longitude numeric
                                               );""")

time_table_create = ("""CREATE TABLE time (start_time timestamp PRIMARY KEY,
                                           hour       int,
                                           day        int,
                                           week       int,
                                           month      int,
                                           year       int,
                                           weekday    int
                                          );""")

# STAGING TABLES

staging_events_copy = ("""COPY staging_events
                          FROM {}
                      IAM_ROLE {}
                          JSON {};
                          """).format(config['S3']['LOG_DATA'],
                                      config['IAM_ROLE']['ARN'],
                                      config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""COPY staging_songs
                         FROM {}
                     IAM_ROLE {}
                         JSON 'auto';
                         """).format(config['S3']['SONG_DATA'],
                                     config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays
                                   (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                            SELECT DISTINCT timestamp 'epoch' + ts/1000 * interval '1 second' AS start_time,
                                            e.user_id,
                                            e.level,
                                            s.song_id,
                                            s.artist_id,
                                            e.session_id,
                                            e.location,
                                            e.user_agent
                              FROM staging_events e, staging_songs s
                             WHERE e.page='NextSong' AND s.title = e.song AND
                                   s.artist_name = e.artist AND s.duration = e.length;
""")

# Some users at some point in time had level=free/paid and changed it to paid/free later.
# Thus, if DISTINCT is used for user_id, these users will be stored more than once.
# In order to avoid this, a subquery is created where each user's timestamps are ranked
# in descending order so we can easily get the latest level from this resulting table by
# tanking into account only the cases where rank=1 (largest timestamp=more recent event).
user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)
                        SELECT user_id,
                               first_name,
                               last_name,
                               gender,
                               level
                          FROM (SELECT *, RANK () OVER (PARTITION BY user_id ORDER BY ts DESC) as rank
                                  FROM staging_events
                                 WHERE page = 'NextSong')
                         WHERE rank = 1;
""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration)
                        SELECT DISTINCT song_id,
                                        title,
                                        artist_id,
                                        year,
                                        duration
                          FROM staging_songs;
""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude)
                          SELECT DISTINCT artist_id,
                                          artist_name AS name,
                                          artist_location AS location,
                                          artist_latitude AS latitude,
                                          artist_longtitude AS longitude
                            FROM staging_songs;
""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
                        SELECT DISTINCT start_time,
                                        EXTRACT(HOUR from start_time) AS hour,
                                        EXTRACT(DAY from start_time) AS day,
                                        EXTRACT(WEEK from start_time) AS week,
                                        EXTRACT(MONTH from start_time) AS month,
                                        EXTRACT(YEAR from start_time) AS year,
                                        EXTRACT(WEEKDAY from start_time) AS weekday
                          FROM (SELECT timestamp 'epoch' + ts/1000 * interval '1 second' AS start_time, page
                                  FROM staging_events
                               )
                         WHERE page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create,
                        songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop,
                      songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert,
                        song_table_insert, artist_table_insert, time_table_insert]
