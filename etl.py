import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.types import *
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.0') \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = '{}/song_data/*/*/*'.format(input_data)
    
    # read song data file
    df = spark.read.json(song_data) # change this to

    # extract columns to create songs table
    songs_table = df[['song_id', 'title', 'artist_id', 'year', 'duration']]
    
    # where to write songs_table
    songs_table_loc = '{}/{}'.format(output_data, 'songs_table')
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write().parquet(songs_table_loc, partitionBy=['year', 'artist_id'])

    # extract columns to create artists table
    artists_table = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']]
    
    # where to write artists_table
    artists_table_loc = '{}/{}'.format(output_data, 'artists_table')
    
    # write artists table to parquet files
    artists_table.write().parquet(artists_table_loc)


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = '{}/log_data/*/*/'.format(input_data)

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    
    # where to write users_table
    users_table_loc = '{}/{}'.format(output_data, 'users_table')
    
    # write users table to parquet files
    users_table.write().parquet(users_table_loc)

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000.), TimestampType())
    log_df = log_df.withColumn('start_time', get_timestamp('ts'))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x/1000.), DateType())
    log_df = log_df.withColumn('datetime', get_datetime('ts'))
    
    # extract columns to create time table
    time_table = df.select('start_time',
                   hour('start_time').alias('hour'),
                   dayofmonth('start_time').alias('day'),
                   weekofyear('start_time').alias('week'),
                   month('start_time').alias('month'),
                   year('start_time').alias('year'),
                   date_format('start_time', 'u').alias('weekday')
                  )
    
    # write time table to parquet files partitioned by year and month
    time_table.write().parquet(output, partitionBy=['year', 'month'])

    # read in song data to use for songplays table
    song_df = spark.read.json('{}/song_data/*/*/*'.format(input_data))
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = song_df.join(df, song_df.artist_name == df.artist)
    songplays_table = [
        ['songplay_id', 'start_time', 'user_id', 
         'level', 'song_id', 'artist_id', 'session_id', 
         'location', 'user_agent']
    ]
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write().parquet(output, partitionBy=['year', 'month'])


def main():
    spark = create_spark_session()
    input_data = 's3a://udacity-dend/'
    output_data = 's3a://abdulellah-dend-project4/'
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == '__main__':
    main()
