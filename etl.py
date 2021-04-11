#libraries needed
import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StringType, IntegerType, StructType, StructField,FloatType
from pyspark.sql import SQLContext

config = configparser.ConfigParser()
config.read('dl.cfg')

#Get our AWS parameters needed for getting the data for S3
os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    """
        Function that creates and returns a spark session
    """
   # spark = SparkSession \
   #    .builder \
    #    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0") \
    #    .getOrCreate()
    
    spark = SparkSession \
        .builder \
        .appName("Data Wrangling with Spark SQL") \
       	.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark



def process_song_data(spark, input_data, output_data):
    """
        In this function we are loading the song_data file and create tables for artist and song.
        Input: Sparksession, 
               Input_data filepath for songs data 
               Output_data filepath for songs data
               
        Output: We produce two parquet files for artist and song tables.
    """
    #shema for the song_data
    schema = StructType([
      StructField("artist_id", StringType(), False),
      StructField("artist_latitude", FloatType(), True),
        StructField("artist_location", StringType(), True),
        StructField("artist_longitude", FloatType(), True),
        StructField("artist_name", StringType(), True),
        StructField("duration", FloatType(), True),
        StructField("num_songs", IntegerType(), True),
        StructField("song_id", StringType(), True),
        StructField("title", StringType(), True),
        StructField("year", IntegerType(), True)])
    
    # get filepath to song data file
    song_data = input_data
    
    # read song data file
    df =  spark.read.json(song_data,schema = schema)

    # extract columns to create songs table
    songs_table =  df.select(col("song_id"),col("title"),col("artist_id"),col("year"),col("duration"))
    
    # write songs table to parquet files partitioned by year and artist
    songs_table =  songs_table.write.partitionBy('year', 'artist_id').parquet(os.path.join(output_data, 'songs.parquet'), 'overwrite')
    print("songs_table partitioned!")

    # extract columns to create artists table
    artists_table = df.select(col("artist_id"),col("artist_name"),col("artist_location"),col("artist_latitude"),col("artist_longitude"))
    # drop duplicates
    artists_table = artists_table.drop_duplicates(subset=['artist_id'])
    # write artists table to parquet files
    artists_table = artists_table.write.partitionBy('artist_id').parquet(os.path.join(output_data, 'artists.parquet'), 'overwrite')
    print("artists_table partitioned!")
    
def tstodatetime(ts):
    """
        Function that transforms milisecond time to a datetimeform (needed for process_log_data function)
    """
    ts_datetime = datetime.fromtimestamp(ts/1000).strftime('%Y-%m-%d %H:%M:%S')
    return ts_datetime

def process_log_data(spark, input_data, output_data):
    """
        In this function we are loading the song_data file and create tables for songplays,users and time tables.
        Input: Sparksession, 
               Input_data filepath for songs data 
               Output_data filepath for songs data
               
        Output: We produce parquet files for songplays,users and time tables.
    """
    # get filepath to log data file
    log_data = input_data
    
    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.where(col("page") == "NextSong")

    # extract columns for users table    
    users_table = df[ 'userId', 'firstName' , 'lastName' ,'gender', 'level'] 
    #drop duplicates
    users_table = users_table.drop_duplicates(subset=['userId'])
    
    # write users table to parquet files
    users_table = users_table.write.partitionBy('userId').parquet(os.path.join(output_data, 'users.parquet'), 'overwrite')
    print("users_table partitioned!")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: tstodatetime(x))
    df = df.withColumn('daytime', get_timestamp(col("ts")))
    
    # extract columns to create time table
    time_table = df.select(
        
    col("ts").alias('start_time'),
    year('daytime').alias('year'), 
    month('daytime').alias('month'), 
    dayofmonth('daytime').alias('day'),
    hour('daytime').alias('hour'), 
    weekofyear('daytime').alias('weekofyear')
        
    )
    #We are going to partition later in the code!

    # read in song data to use for songplays table
    sqlContext = SQLContext(spark)
    songs_table = sqlContext.read.parquet('data/outputs/song_data/songs.parquet') 

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df['ts', 'userId', 'level','sessionId', 'location', 'userAgent','song']
    #add artists id and song id by joining with songs_table
    songplays_table = songplays_table.alias('s').join(songs_table.alias('e'),col('e.title') == col('s.song'))\
    .select(col('s.ts').alias('start_time'),
        col('s.userId'),
        col('s.level'),
        col('s.sessionId'),
        col('s.location'),
        col('s.userAgent'),
        col('s.song'),
        col('e.artist_id').alias('artist_id'),
        col('e.song_id').alias('song_id'))
    #add month and year for partitioning later based on those
    time_table_short = time_table['start_time','month','year']
    songplays_table = songplays_table.alias('s').join(time_table_short.alias('t'),col('t.start_time') == col('s.start_time'))\
    .select(col('s.start_time'),
        col('s.userId'),
        col('s.level'),
        col('s.sessionId'),
        col('s.location'),
        col('s.userAgent'),
        col('s.song'),
        col('s.artist_id'),
        col('s.song_id'),
        col('t.year'),
        col('t.month'),
       )
    # write time table to parquet files partitioned by year and month
    time_table = time_table.write.partitionBy('year','month').parquet(os.path.join(output_data, 'times.parquet'), 'overwrite')
    print("time_table partitioned!")
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table = songplays_table.write.partitionBy('year','month').parquet(os.path.join(output_data, 'songplays.parquet'), 'overwrite')
    print("songplays_table partitioned!")


def main():
    spark = create_spark_session()
    #datasource for local song_data file (in case of AWS connection this will be the S3 bucket!)
    
    #Input for local (uncomment if you want to use)
    #input_data = 'data/song_data/*/*/*/*.json'
    
    #Input for whole S3 bucket  (uncomment if you want to use)
    #input_data = 's3a://udacity-dend/song_data/*/*/*/*.json'
    
    #Input for part of S3 bucket
    input_data = 's3a://udacity-dend/song_data/A/A/*/*.json'
    output_data = 'data/outputs/song_data'
    
    process_song_data(spark, input_data, output_data)
    
    #datasource for local log_data file (in case of AWS connection this will be the S3 bucket!)
    
    #Input for local (uncomment if you want to use)
    #input_data = 'data/logs_data/*.json'
    
    #Input for  S3 bucket
    input_data =  's3a://udacity-dend/log-data/*/*/*.json'
    output_data = 'data/outputs/log_data'
    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
