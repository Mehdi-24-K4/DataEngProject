from pyspark.sql import SparkSession
from constantes import MONGO_URI, MONGO_DB, MONGO_COLLECTION
from pyspark.sql.functions import col, explode, year, month, dayofmonth, hour
from sqlalchemy import create_engine
def read_data_from_mongodb(spark_session):
    """
    Reads data from MongoDB using the provided Spark session and returns a DataFrame.

    Parameters:
        spark_session (SparkSession): The Spark session used to read data from MongoDB.

    Returns:
        DataFrame: A DataFrame containing the data read from MongoDB.
    """
    mongo_uri = f"{MONGO_URI}{MONGO_DB}.{MONGO_COLLECTION}"
    data_frame = spark_session.read.format("com.mongodb.spark.sql.DefaultSource") \
        .option("uri", mongo_uri) \
        .load()
    
    return data_frame

def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Air Quality Data Processing") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1,org.postgresql:postgresql:42.2.18") \
        .getOrCreate()

    # Read data from MongoDB
    df = read_data_from_mongodb(spark)
    
    # Display schema for verification
    df.printSchema()

    # Explode the 'measurements' column
    df_exploded = df.withColumn("measurement", explode("measurements"))

    # Select and rename necessary columns for the fact table
    df_measurements = df_exploded.select(
        col("_id.oid").alias("measurement_id"),
        col("city"),
        col("country"),
        col("location").alias("location_name"),
        col("coordinates.latitude").alias("latitude"),
        col("coordinates.longitude").alias("longitude"),
        col("measurement.parameter").alias("parameter_name"),
        col("measurement.value").alias("value"),
        col("measurement.unit").alias("unit"),
        col("measurement.lastUpdated").alias("timestamp")
    )

    # Create dimension tables
    df_location = df_measurements.select("location_name", "city", "country", "latitude", "longitude").distinct()
    df_parameter = df_measurements.select("parameter_name").distinct()
    df_time = df_measurements.select("timestamp").distinct() \
        .withColumn("year", year("timestamp")) \
        .withColumn("month", month("timestamp")) \
        .withColumn("day", dayofmonth("timestamp")) \
        .withColumn("hour", hour("timestamp"))

    # Join dimensions to create fact table
    df_facts = df_measurements \
        .join(df_location, ["location_name", "city", "country", "latitude", "longitude"], "inner") \
        .join(df_parameter, ["parameter_name"], "inner") \
        .join(df_time, ["timestamp"], "inner")

    # Rename columns for final fact table
    df_facts = df_facts.withColumnRenamed("location_name", "location_id") \
        .withColumnRenamed("parameter_name", "parameter_id") \
        .withColumnRenamed("timestamp", "time_id")

    # Select final columns for fact table
    df_facts = df_facts.select(
        col("measurement_id"),
        col("location_id"),
        col("time_id"),
        col("parameter_id"),
        col("value"),
        col("unit")
    )

    # PostgreSQL connection properties
    postgres_properties = {
        "user": "admin",
        "password": "pass123",
        "driver": "org.postgresql.Driver"
    }
    postgres_url = "jdbc:postgresql://localhost:5432/air_quality"

    # Write data to PostgreSQL
    print("Saving data to PostgreSQL.")
    df_location.write.jdbc(url=postgres_url, table="dimension_location", mode="overwrite", properties=postgres_properties)
    df_parameter.write.jdbc(url=postgres_url, table="dimension_parameter", mode="overwrite", properties=postgres_properties)
    df_time.write.jdbc(url=postgres_url, table="dimension_time", mode="overwrite", properties=postgres_properties)
    df_facts.write.jdbc(url=postgres_url, table="air_quality_measurements", mode="overwrite", properties=postgres_properties)

    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()



# [Environment]::SetEnvironmentVariable("HADOOP_HOME", "C:\Users\amine\OneDrive\Bureau\ProjetsMehdi\ProjetDataEngMehdi\hadoop-3.4.0-src\dev-support", "User")
# [Environment]::SetEnvironmentVariable("HADOOP_HOME", "C:\Users\amine\OneDrive\Bureau\ProjetsMehdi\ProjetDataEngMehdi\hadoop-3.4.0-src\dev-support\bin", "amine")
# spark-submit --jars file:///C:/Users/amine/OneDrive/Bureau/ProjetsMehdi/Projet2/MongoSparkConnector/mongo-spark-connector_2.12-3.0.1.jar,file:///C:/Users/amine/OneDrive/Bureau/ProjetsMehdi/Projet2/MongoSparkConnector/bson-4.0.5.jar testSpark.py
