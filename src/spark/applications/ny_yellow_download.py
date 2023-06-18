from pyspark.sql import SparkSession
import wget
import pendulum
import os
import sys

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
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{pen.format('YYYY-MM')}.parquet"
    local_path = f"/shared-data/yellow_tripdata_{pen.format('YYYY_MM')}.parquet"


    # initialize log4j
    log4jLogger = spark.sparkContext._jvm.org.apache.log4j
    LOGGER = log4jLogger.LogManager.getLogger(__name__)

    LOGGER.info("Start running Application")
    LOGGER.info(f"Download ny yellow taxi to a local storage: {local_path}")

    if os.path.exists(local_path):
        os.remove(local_path)
    wget.download(url, local_path)

if __name__ == "__main__":
    main()
