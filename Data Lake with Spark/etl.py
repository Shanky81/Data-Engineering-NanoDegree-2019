import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')
#conf.set("spark.ui.port", "4042")
         
os.environ['AWS_ACCESS_KEY_ID']=config['AWS_KEY']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_KEY']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    
    """
        Description: This function loads song_data from S3 and processes it by extracting the songs and artist tables and then loaded back to S3 db.
        
        Parameters:
            spark       : Spark Session
            input_data  : Source data path of song_data from where the file is being loaded and processed.
            output_data : Destination location where processed the results will be stored 
            
    """
    # get filepath to song data file
    song_data = os.path.join(input_data, "song_data/A/A/A/*.json")
    
    # read song data file
    df =spark.read.json(song_data)

    # created song view to write SQL Queries
    df.createOrReplaceTempView("tbl_song_data")
    
    # extract columns to create songs table
    tbl_songs = spark.sql(""" SELECT sntbl.song_id, sntbl.title, sntbl.artist_id,
                            sntbl.year, sntbl.duration
                            FROM tbl_song_data sntbl
                            WHERE sntbl.song_id IS NOT NULL """)
    
    # write songs table to parquet files partitioned by year and artist
    tbl_songs.write.partitionBy("year","artist_id").parquet(os.path.join(output_data, 'tbl_songs.parquet'), 'overwrite')

    # extract columns to create artists table
    tbl_artists = spark.sql("""SELECT DISTINCT arttbl.artist_id, arttbl.artist_name,
                                arttbl.artist_location, arttbl.artist_latitude,
                                arttbl.artist_longitude
                                FROM tbl_song_data arttbl
                                WHERE arttbl.artist_id IS NOT NULL""") 
    
    # write artists table to parquet files
    tbl_artists.write.parquet(os.path.join(output_data, 'tbl_artists.parquet'), 'overwrite')


def process_log_data(spark, input_data, output_data):
    """
        Description: This function is loading log_data from S3 and processes it by extracting (songs, artist) tables and then again loaded back to S3. 
        Also output from previous function is used in by spark.read.json command
        
        Parameters:
            spark       : Spark Session
            input_data  : Source data path of song_data from where the file is being loaded and processed.
            output_data : Destination location where processed the results will be stored

    """
    
    # get filepath to log data file
    log_data = os.path.join(input_data,"log_data/*/*/*.json")

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df =  df.filter(df.page == 'NextSong')

    # created log view to write SQL Queries
    df.createOrReplaceTempView("tbl_log_data")
    
    # extract columns for users table    
    tbl_users = spark.sql(""" SELECT DISTINCT usrlog.userId as user_id, 
                            usrlog.firstName as first_name, usrlog.lastName as last_name,
                            usrlog.gender as gender,
                            usrlog.level as level
                            FROM tbl_log_data usrlog
                            WHERE usrlog.userId IS NOT NULL
                        """)
    
    # write users table to parquet files
    #tbl_users.write.parquet(os.path.join(output_data, 'tbl_users.parquet'), 'overwrite')
    tbl_users.write.mode('overwrite').parquet(output_data+'tbl_users/')
    # create timestamp column from original timestamp column
    #get_timestamp = udf()
    #df = 

    
    # extract columns to create time table
    tbl_time = spark.sql("""SELECT to_timestamp(ts/1000) as start_time, hour(to_timestamp(ts/1000)) as hour, dayofmonth(to_timestamp(ts/1000)) as day, weekofyear(to_timestamp(ts/1000)) as week, month(to_timestamp(ts/1000)) as month, year(to_timestamp(ts/1000)) as year, dayofweek(to_timestamp(ts/1000)) as weekday FROM tbl_log_data where ts IS NOT NULL""")
    
    # write time table to parquet files partitioned by year and month
    tbl_time.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'tbl_time.parquet'), 'overwrite')

    # read in song data to use for songplays table
    song_data = os.path.join(input_data, "song-data/A/A/A/*")
    song_df = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table 
    tbl_songplays =  spark.sql(""" SELECT monotonically_increasing_id() as songplay_id, to_timestamp(logtbl.ts/1000) as start_time, month(to_timestamp(logtbl.ts/1000)) as month, year(to_timestamp(logtbl.ts/1000)) as year, logtbl.userId as user_id, logtbl.level as level, sngtbl.song_id as song_id, sngtbl.artist_id as artist_id, logtbl.sessionId as session_id, logtbl.location as location, logtbl.userAgent as user_agent FROM tbl_log_data logtbl INNER JOIN tbl_song_data sngtbl on logtbl.artist = sngtbl.artist_name and logtbl.song = sngtbl.title """)

    # write songplays table to parquet files partitioned by year and month
    tbl_songplays.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'tbl_songplays.parquet'), 'overwrite')
    print("process completed")

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-sh/datalakeoutput/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
