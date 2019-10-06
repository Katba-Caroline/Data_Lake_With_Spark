import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

# os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
# os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS','AWS_SECRET_ACCESS_KEY')

def create_spark_session():
    """
    Create a Spark Session
    """
    spark = SparkSession\
    .builder\
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.5")\
    .getOrCreate()
    #     print (spark.sparkContext.getConf().getAll)
    return spark
#  .appName("Sparkify_Spark_Session")\
#         .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \

def process_song_data(spark, input_data, output_data): 
    
#      """
#      This function loads song_data from S3,
#      processes it by extracting songs and artist tables
#      then, loads it back to S3
        
#         Parameters:
#             spark       : The Spark Session
#             input_data  : The location of the song_data json files
#             output_data : S3 bucket where data is transformed into dimensional tables and stored as parquet files
#     """
    
    # filepath to song data file
#     song_data = input_data + "song_data/*/*/*/*.json"
#     song_data = input_data + "song_data/*/*/*"
    # get filepath to song data file

    song_data = os.path.join(input_data, "song_data/A/*/*/*.json")

    print(song_data)
    
    # read song data file
#     df = spark.read.json(song_data)
    df = spark.read.format("json").load("s3a://udacity-dend/song_data/*/*/*")

    # extract columns to create songs table
    songs_table = (
        df.select(
            'song_id', 'title', 'artist_id',
            'year', 'duration'
        ).distinct()
    )
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data + "songs.parquet", mode="overwrite")

    # extract columns to create artists table
    artists_table = (
        df.select(
            'artist_id',
            col('artist_name').alias('name'),
            col('artist_location').alias('location'),
            col('artist_latitude').alias('latitude'),
            col('artist_longitude').alias('longitude'),
        ).distinct()
    )
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artists.parquet", mode="overwrite")
print("Processing Song and Artist Data ")


def process_log_data(spark, input_data, output_data):
#     """
#     This function processes all event logs of the Sparkify app, with special emphasis on page'NextSong'.
        
#         Parameters:
#             spark       : The Spark Session
#             input_data  : The location of log_data json files
#             output_data :S3 bucket where data is transformed into dimensional tables and stored as parquet files
            
#     """
    #filepath to log data file
#     log_data = input_data + "log_data/*/*" 

    log_data = os.path.join(input_data, "log_data/*/*/*.json")

    print(log_data)
    # read log data file
#     df = spark.read.json(log_data)
    
    df = spark.read.format("json").load("s3a://udacity-dend/log_data/*/*")
    
    # filter by actions for song plays
#     df = df.where(df.page == 'NextSong')
    df = df.filter(df.page == 'NextSong').dropDuplicates()

    

    # extract columns for users table    
    users_table = (
        df.select(
            col('userId').alias('user_id'),
            col('firstName').alias('first_name'),
            col('lastName').alias('last_name'),
            col('gender').alias('gender'),
            col('level').alias('level')
        ).distinct()
    )
    
    # write users table to parquet files
    users_table.write.parquet(output_data + "users.parquet", mode="overwrite")

    # create timestamp column from original timestamp column
#     get_timestamp = udf()
#     df = 
    
    # create datetime column from original timestamp column
    get_datetime = udf(date_convert, TimestampType())
    df = df.withColumn("start_time", get_datetime('ts'))
    
    # extract columns to create time table
    time_table = (
        df.withColumn("hour", hour(col("start_time")))
          .withColumn("day", dayofmonth(col("start_time")))
          .withColumn("week", weekofyear(col("start_time")))
          .withColumn("month", month(col("start_time")))
          .withColumn("year", year(col("start_time")))
          .withColumn("weekday", date_format(col("start_time"), 'E'))
          .select(
            col("start_time"),
            col("hour"),
            col("day"),
            col("week"),
            col("month"),
            col("year"),
            col("weekday")
          )
    )
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_data + "time.parquet", mode="overwrite")

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + "songs.parquet")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table =  (
        df.withColumn("songplay_id", F.monotonically_increasing_id())
          .join(song_df, song_df.title == df.song)
          .select(
            "songplay_id",
            col("ts_timestamp").alias("start_time"),
            col("userId").alias("user_id"),
            "level",
            "song_id",
            "artist_id",
            col("sessionId").alias("session_id"),
            "location",
            col("userAgent").alias("user_agent")
          )
    )

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(output_data + "songplays.parquet", mode="overwrite")
print("Processing Log Data")

def main():
    spark = create_spark_session()
    print (spark.sparkContext.getConf().getAll)
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkify-data-lake-dend/"
    
    process_song_data(spark, input_data, output_data)    
    print("Finished Processing Song and Artist Data ")
    process_log_data(spark, input_data, output_data)
    print("Finished Processing Log Data")

if __name__ == "__main__":
    main()
