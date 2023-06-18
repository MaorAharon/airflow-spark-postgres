from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, LongType, TimestampType, \
    IntegerType, DateType

from pyspark.sql import SparkSession
import os
from pyspark.sql.functions import lit
from pyspark.sql.functions import to_date
from spark.applications.ny_yellow_load import set_schema


def main():
    # Create a Spark session
    expected_schema = StructType([
        StructField("vendor_id", LongType(), True),
        StructField("tpep_pickup_datetime", TimestampType(), True),
        StructField("tpep_dropoff_datetime", TimestampType(), True),
        StructField("passenger_count", DoubleType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("ratecode_id", DoubleType(), True),
        StructField("store_and_fwd_flag", StringType(), True),
        StructField("pu_location_id", LongType(), True),
        StructField("do_location_id", LongType(), True),
        StructField("payment_type", LongType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("extra", DoubleType(), True),
        StructField("mta_tax", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("tolls_amount", DoubleType(), True),
        StructField("improvement_surcharge", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("congestion_surcharge", DoubleType(), True),
        StructField("airport_fee", IntegerType(), True),
        StructField("date", DateType(), True),
    ])
    spark: SparkSession = SparkSession.builder \
        .appName("PySpark Example Project") \
        .getOrCreate()


    # Get the directory of the current script (__file__)
    current_script_folder = os.path.dirname(os.path.abspath(__file__))
    spark_dir = os.path.abspath(os.path.join(current_script_folder, os.pardir))
    p_1 = f"{spark_dir}/data/parquet/yellow_tripdata_2020-01.parquet"
    df = spark.read.parquet(p_1) \
        .withColumn("date", to_date(lit(('2020-02-03')), "yyyy-MM-dd"))
    df_2 = set_schema(df)

    assert df_2.schema == expected_schema, f"Expected schema: {expected_schema}, but got: {df_2.schema}"

    schema = StructType([
        StructField("column1", StringType(), True),
        StructField("column2", StringType(), True),
        StructField("date", DateType(), True)
    ])

    # Create an empty DataFrame with the schema
    empty_df = spark.createDataFrame([], schema)
    df_3 = set_schema(empty_df)
    assert df_3.schema == expected_schema, f"Expected schema: {expected_schema}, but got: {df_3.schema}"

if __name__ == "__main__":
    main()
