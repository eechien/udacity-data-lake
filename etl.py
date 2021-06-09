import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    date_format,
    dayofmonth,
    dayofweek,
    from_unixtime,
    hour,
    monotonically_increasing_id,
    month,
    to_timestamp,
    weekofyear,
    year,
    udf,
)
from pyspark.sql.types import (
    TimestampType as Time,
    FloatType as Flt,
    IntegerType as Int,
    StringType as Str,
    StructField as Fld,
    StructType as R,
)


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config.get("AWS", "AWS_ACCESS_KEY_ID")
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get("AWS", "AWS_SECRET_ACCESS_KEY")

OUTPUT_BUCKET = config.get("S3", "OUTPUT_BUCKET")

def create_spark_session():
    """
    Setup spark session.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def read_song_data(spark, input_data):
    """
    Read song data from the input bucket and process itinto a DataFrame 
    via Spark.
    """
    song_data = f"{input_data}song_data/*/*/*/*.json"
    song_schema = R([
        Fld("num_songs", Int()),
        Fld("artist_id", Str(), nullable=False),
        Fld("artist_latitude", Flt()),
        Fld("artist_longitude", Flt()),
        Fld("artist_location", Str()),
        Fld("artist_name", Str(), nullable=False),
        Fld("song_id", Str(), nullable=False),
        Fld("title", Str(), nullable=False),
        Fld("duration", Flt(), nullable=False),
        Fld("year", Int(), nullable=False),
    ])
    df = spark.read.json(song_data, schema=song_schema)
    return df


def create_song_table(df, output_data):
    """
    Create the song table from the song DataFrame and write it to parquet
    files on the S3 output bucket, partitioned by year and artist.
    """
    songs_table = df["song_id", "title", "artist_id", "year", "duration"]
    songs_table.write.parquet(
        f"{output_data}song",
        mode="overwrite",
        partitionBy=["year", "artist_id"]
    )

    
def create_artist_table(df, output_data):
    """
    Create the artist table from the song DataFrame and write it to
    parquet files on the S3 output bucket.
    """
    artists_table = df[
        "artist_id",
        "artist_name",
        "artist_location",
        "artist_latitude",
        "artist_longitude"
    ]
    artists_table = artists_table.withColumnRenamed("artist_name", "name")\
        .withColumnRenamed("artist_location", "location")\
        .withColumnRenamed("artist_latitude", "latitude")\
        .withColumnRenamed("artist_longitude", "longitude")
    artists_table.write.parquet(f"{output_data}artist", mode="overwrite")
    
    
def process_song_data(spark, df, input_data, output_data):
    """
    Write the song and artist tables to parquet files on the S3 bucket
    from the song DataFrame.
    """
    create_song_table(df, output_data)
    create_artist_table(df, output_data)


def create_user_table(df, output_data):
    """
    Create the user table from the event log DataFrame and write it
    to parquet files on the S3 output bucket.
    """
    users_table = df[
        col("userId").cast(Int()),
        "firstName",
        "lastName",
        "gender",
        "level"
    ].distinct()
    users_table = users_table.withColumnRenamed("userId", "user_id")\
        .withColumnRenamed("firstName", "first_name")\
        .withColumnRenamed("lastName", "last_name")
    users_table.write.parquet(f"{output_data}user", mode="overwrite")


def create_time_table(df, output_data):
    """
    Create the time table from the event log DataFrame and write it
    to parquet files on the S3 output bucket, partitioned by year
    and month.
    """
    time_table = df[
        "start_time",
        hour("start_time"),
        dayofmonth("start_time"),
        weekofyear("start_time"),
        month("start_time"),
        year("start_time"),
        dayofweek("start_time"),
    ].distinct()
    time_table = time_table.withColumnRenamed("hour(start_time)", "hour")\
        .withColumnRenamed("dayofmonth(start_time)", "day")\
        .withColumnRenamed("weekofyear(start_time)", "week")\
        .withColumnRenamed("month(start_time)", "month")\
        .withColumnRenamed("year(start_time)", "year")\
        .withColumnRenamed("dayofweek(start_time)", "weekday")
    time_table.write.parquet(
        f"{output_data}time",
        mode="overwrite",
        partitionBy=["year", "month"]
    )
    return time_table
    

def create_songplay_table(spark, time_table, song_df, log_df, output_data):
    """
    Create the songplay table from joining the song DataFrame, event log DataFrame
    and time table and write it to the S3 output bucket, partitioned by year and month.
    """
    time_table.createOrReplaceTempView("time")
    song_df.createOrReplaceTempView("song")
    log_df.createOrReplaceTempView("eventLog")
    
    songplays_table = spark.sql(
        """
        SELECT
            e.start_time,
            e.userId as user_id,
            e.level,
            s.song_id,
            s.artist_id,
            e.sessionId as session_id,
            e.location,
            e.userAgent as user_agent,
            t.year,
            t.month
        FROM eventLog e
        JOIN song s ON (s.title = e.song AND e.artist = s.artist_name)
        JOIN time t ON (e.start_time = t.start_time)
        """
    )
    songplays_table = songplays_table.withColumn(
        "songplay_id", monotonically_increasing_id()
    )
    songplays_table.write.parquet(
        f"{output_data}songplay",
        mode="overwrite",
        partitionBy=["year", "month"],
    )


def process_log_data(spark, song_df, input_data, output_data):
    """
    Write the user, time, and songplay tables to parquet files on the
    S3 output bucket from the song and event log data.
    """
    log_data = f"{input_data}log_data/*/*/*-events.json"
    df = spark.read.json(log_data)
    df = df.filter(df.page == "NextSong")
    get_timestamp = udf(lambda x: datetime.utcfromtimestamp(x / 1000), Time())
    df = df.withColumn('start_time', get_timestamp(col('ts')))
    
    create_user_table(df, output_data)
    time_table = create_time_table(df, output_data)
    create_songplay_table(spark, time_table, song_df, df, output_data)


def main():
    """
    Connects to Spark and reads song and event log data from an S3 bucket.
    Processes the data into song, artist, user, time, and songplay tables.
    Writes those tables to parquet files on another S3 bucket.
    """
    
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = f"s3a://{OUTPUT_BUCKET}/"
    
    song_df = read_song_data(spark, input_data)
    process_song_data(spark, song_df, input_data, output_data)    
    process_log_data(spark, song_df, input_data, output_data)
    spark.stop()


if __name__ == "__main__":
    main()
