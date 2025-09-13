from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
import sys
import logging

def main(input_path, mysql_url, mysql_user, mysql_password):
    logging.basicConfig(level=logging.INFO)
    spark = None

    try:
        logging.info("Starting Spark session...")
        spark = SparkSession.builder \
            .appName("YellowTaxiETL") \
            .getOrCreate()

        logging.info(f"Reading CSV from: {input_path}")
        df = spark.read.option("header", True).csv(input_path)

        # Validate required columns
        required_columns = [
            "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
            "passenger_count", "trip_distance", "payment_type", "fare_amount"
        ]
        missing = [col for col in required_columns if col not in df.columns]
        if missing:
            raise ValueError(f"Missing required columns: {missing}")

        # Basic cleaning
        df = df.withColumn("tpep_pickup_datetime", to_date(col("tpep_pickup_datetime"))) \
               .withColumn("tpep_dropoff_datetime", to_date(col("tpep_dropoff_datetime"))) \
               .filter(col("fare_amount") > 0)

        logging.info("Writing cleaned data to MySQL...")
        df.write.format("jdbc") \
            .option("url", mysql_url) \
            .option("dbtable", "yellow_taxi_trips") \
            .option("user", mysql_user) \
            .option("password", mysql_password) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("createTableColumnTypes",
                    "VendorID INT, "
                    "tpep_pickup_datetime DATE, "
                    "tpep_dropoff_datetime DATE, "
                    "passenger_count INT, "
                    "trip_distance DOUBLE, "
                    "payment_type INT, "
                    "fare_amount DOUBLE") \
            .mode("overwrite") \
            .save()

        logging.info("ETL job completed successfully.")

    except Exception as e:
        logging.error(f"ETL job failed: {e}")
        raise

    finally:
        if spark:
            spark.stop()
            logging.info("Spark session stopped.")

if __name__ == "__main__":
    if len(sys.argv) != 5:
        logging.error("Usage: etl_yellow_taxi.py <input_path> <mysql_url> <mysql_user> <mysql_password>")
        sys.exit(1)

    input_path = sys.argv[1]
    mysql_url = sys.argv[2]
    mysql_user = sys.argv[3]
    mysql_password = sys.argv[4]

    main(input_path, mysql_url, mysql_user, mysql_password)
