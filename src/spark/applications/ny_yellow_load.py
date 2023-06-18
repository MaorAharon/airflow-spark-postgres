from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, LongType, TimestampType, IntegerType
from pyspark.sql.functions import col
from pyspark.sql.functions import lit
from pyspark.sql.functions import to_date
from pyspark.sql import SparkSession
import pendulum
import os
import sys


def select_or_null(df, col_name):
    if col_name in df.columns:
        return df[col_name]
    else:
        return lit(None).alias(col_name)

def set_schema(df):
    return df.select(select_or_null(df, "VendorID").cast(LongType()).alias("vendor_id"),
              select_or_null(df, "tpep_pickup_datetime").cast(TimestampType()),
              select_or_null(df, "tpep_dropoff_datetime").cast(TimestampType()),
              select_or_null(df, "passenger_count").cast(DoubleType()),
              select_or_null(df, "trip_distance").cast(DoubleType()),
              select_or_null(df, "RatecodeID").cast(DoubleType()).alias("ratecode_id"),
              select_or_null(df, "store_and_fwd_flag").cast(StringType()),
              select_or_null(df, "PULocationID").cast(LongType()).alias("pu_location_id"),
              select_or_null(df, "DOLocationID").cast(LongType()).alias("do_location_id"),
              select_or_null(df, "payment_type").cast(LongType()),
              select_or_null(df, "fare_amount").cast(DoubleType()),
              select_or_null(df, "extra").cast(DoubleType()),
              select_or_null(df, "mta_tax").cast(DoubleType()),
              select_or_null(df, "tip_amount").cast(DoubleType()),
              select_or_null(df, "tolls_amount").cast(DoubleType()),
              select_or_null(df, "improvement_surcharge").cast(DoubleType()),
              select_or_null(df, "total_amount").cast(DoubleType()),
              select_or_null(df, "congestion_surcharge").cast(DoubleType()),
              select_or_null(df, "airport_fee").cast(IntegerType()),
              col("date"))

def main():
    # initialize or reuse existing SparkSession
    spark = (SparkSession
             .builder
             .getOrCreate()
             )

    ####################################
    # Parameters
    ####################################
    date_string = sys.argv[1]
    pen: pendulum =  pendulum.parse(date_string)
    local_path = f"/shared-data/yellow_tripdata_{pen.format('YYYY_MM')}.parquet"
    postgres_url = "jdbc:postgresql://postgres:5432/"
    postgres_tbl=f"stage.yellow_tripdata_{pen.format('YYYY_MM')}"
    cnt_tbl = "taxi.ny_yellow_cnt"
    postgres_user='airflow'
    postgres_pwd='airflow'
    pgsql_jdbc="/shared-data/accessories/postgresql-42.2.6.jar"

    # initialize log4j
    log4jLogger = spark.sparkContext._jvm.org.apache.log4j
    LOGGER = log4jLogger.LogManager.getLogger(__name__)

    LOGGER.info("Start running Application")


    LOGGER.info("Read local file and edjust schema")

    df = spark.read \
        .parquet(f'file://{local_path}') \
        .withColumn("date", to_date(lit(pen.format('YYYY-MM-DD')), "yyyy-MM-dd"))

    df_2 = set_schema(df)
    df_2.cache

    LOGGER.info("Write the file into staging table")
    df_2.limit(10).write\
        .mode("overwrite")\
        .format("jdbc") \
        .option("url", postgres_url) \
        .option("dbtable", postgres_tbl) \
        .option("user", postgres_user) \
        .option("password", postgres_pwd) \
        .option("driver", "org.postgresql.Driver") \
        .option("jar", pgsql_jdbc) \
        .save()

    LOGGER.info("Write the file into count table")
    schema = StructType([StructField("cnt", LongType(), True)])
    cnt = df_2.count()
    one_row_df = spark.createDataFrame([(cnt, )], schema=schema) \
        .withColumn("date", to_date(lit(pen.format('YYYY-MM-DD')), "yyyy-MM-dd"))

    LOGGER.info("Write the file into count table")
    one_row_df.write \
        .jdbc(url=postgres_url,
              table=cnt_tbl,
              mode="append",
              properties={
                  "user": postgres_user,
                  "password": postgres_pwd,
                  "driver": "org.postgresql.Driver"
              })

    LOGGER.info("Delete file")
    os.remove(local_path)

if __name__ == "__main__":
    main()
