from pyspark.sql import SparkSession
from constantes import MONGO_URI, MONGO_DB, MONGO_COLLECTION
from pyspark.sql.functions import col, lower, explode, year, month, dayofmonth, hour, avg, when
from sqlalchemy import create_engine, text
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


def clean_and_prepare_data(df):
    """
    Clean and prepare data for processing.

    Args:
        df (DataFrame): The input DataFrame containing the raw data.

    Returns:
        DataFrame: The cleaned and prepared DataFrame with the following columns:
            - measurement_id (string): The measurement ID.
            - city (string): The city.
            - country (string): The country.
            - location_name (string): The location name.
            - latitude (float): The latitude.
            - longitude (float): The longitude.
            - parameter_name (string): The parameter name (lowercase).
            - value (float): The value.
            - unit (string): The unit.
            - timestamp (datetime): The timestamp.
    """
    df_exploded = df.withColumn("measurement", explode("measurements"))
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
    
    # Convert the parameter_name column to lowercase
    df_measurements = df_measurements.withColumn("parameter_name", lower(col("parameter_name")))
    
    return df_measurements

def create_dimension_tables(df_measurements):
    """
    A function to create dimension tables based on the input dataframe of measurements.
    
    Args:
    df_measurements (DataFrame): The input dataframe containing measurements.

    Returns:
    DataFrame: df_location - A dataframe with location information.
    DataFrame: df_parameter - A dataframe with parameter information.
    DataFrame: df_time - A dataframe with time information.
    """
    df_location = df_measurements.select("location_name", "city", "country", "latitude", "longitude").distinct()
    df_parameter = df_measurements.select("parameter_name","value", "unit").distinct()
    df_time = df_measurements.select("timestamp").distinct() \
        .withColumn("year", year("timestamp")) \
        .withColumn("month", month("timestamp")) \
        .withColumn("day", dayofmonth("timestamp")) \
        .withColumn("hour", hour("timestamp"))
    return df_location, df_parameter, df_time

def create_fact_table(df_measurements, df_location, df_parameter, df_time):
    """
    Create the fact table by joining dimension tables.
    """
    df_facts = df_measurements \
        .join(df_location, ["location_name", "city", "country", "latitude", "longitude"], "inner") \
        .join(df_parameter, ["parameter_name", "value", "unit"], "inner") \
        .join(df_time, ["timestamp"], "inner")
    
    df_facts = df_facts.withColumnRenamed("location_name", "location_id") \
        .withColumnRenamed("parameter_name", "parameter_id") \
        .withColumnRenamed("timestamp", "time_id")
    
    df_facts = df_facts.select(
        col("measurement_id"),
        col("location_id"),
        col("time_id"),
        col("parameter_id"),
        # col("value"),
        # col("unit")
    )
    
    return df_facts

def calculate_daily_aggregates(df_measurements):
    """
    Calculate the daily averages of measurements for each city, country, and parameter on a specific date.

    Args:
    df_measurements (DataFrame): The input dataframe containing measurements.

    Returns:
    DataFrame: The dataframe containing the daily average value of each parameter for each city and country.
    """
    df_daily_avg = df_measurements \
        .withColumn("date", col("timestamp").cast("date")) \
        .groupBy("city", "country", "parameter_name", "date") \
        .agg(avg("value").alias("average_value"))
    return df_daily_avg

def calculate_seasonal_trends(df_measurements):
    """
    Calculate the average value of each parameter for each city and country
    in each season for each year.

    Args:
    df_measurements (DataFrame): The input dataframe containing measurements.

    Returns:
    DataFrame: The dataframe containing the average value of each parameter
    for each city and country in each season for each year.
    """
    # Extract year and month from the timestamp column
    # Rename the columns for simplicity
    df_measurements = df_measurements \
        .withColumn("year", year("timestamp")) \
        .withColumn("month", month("timestamp"))

    # Assign season to each measurement
    # Winter: 12, 1, 2; Spring: 3, 4, 5; Summer: 6, 7, 8; Fall: 9, 10, 11
    # Create a new column 'season' based on the month value
    df_measurements = df_measurements.withColumn(
        "season",
        when(col("month").isin([12, 1, 2]), "Winter")
        .when(col("month").isin([3, 4, 5]), "Spring")
        .when(col("month").isin([6, 7, 8]), "Summer")
        .otherwise("Fall")
    )

    # Calculate the average value of each parameter for each city and country
    # in each season for each year
    # Group by city, country, parameter, year, and season
    # Calculate the average value of the 'value' column
    # Rename the new column as 'average_value'
    df_seasonal_avg = df_measurements \
        .groupBy("city", "country", "parameter_name", "year", "season") \
        .agg(avg("value").alias("average_value"))

    return df_seasonal_avg

def create_tables_in_postgresql(engine):
    """
    Crée les tables dans la base de données PostgreSQL en utilisant l'engine fourni.
    """
    create_dimension_location_table = """
    CREATE TABLE IF NOT EXISTS dimension_location (
        location_id SERIAL PRIMARY KEY,
        location_name VARCHAR(255),
        city VARCHAR(255),
        country VARCHAR(255),
        latitude FLOAT,
        longitude FLOAT
    );
    """

    create_dimension_parameter_table = """
    CREATE TABLE IF NOT EXISTS dimension_parameter (
        parameter_id SERIAL PRIMARY KEY,
        parameter_name VARCHAR(255),
        value FLOAT,
        unit VARCHAR(255)
    );
    """

    create_dimension_time_table = """
    CREATE TABLE IF NOT EXISTS dimension_time (
        time_id SERIAL PRIMARY KEY,
        timestamp TIMESTAMP,
        year INT,
        month INT,
        day INT,
        hour INT
    );
    """

    create_fact_table = """
    CREATE TABLE IF NOT EXISTS air_quality_measurements (
        measurement_id VARCHAR(255),
        location_id INT,
        time_id INT,
        parameter_id INT,
        PRIMARY KEY (measurement_id),
        FOREIGN KEY (location_id) REFERENCES dimension_location (location_id),
        FOREIGN KEY (time_id) REFERENCES dimension_time (time_id),
        FOREIGN KEY (parameter_id) REFERENCES dimension_parameter (parameter_id)
    );
    """

    create_daily_aggregates_table = """
    CREATE TABLE IF NOT EXISTS daily_aggregates (
        city VARCHAR(255),
        country VARCHAR(2),
        parameter_name VARCHAR(255),
        date DATE,
        average_value FLOAT
    );
    """

    create_seasonal_trends_table = """
    CREATE TABLE IF NOT EXISTS seasonal_trends (
        city VARCHAR(255),
        country VARCHAR(2),
        parameter_name VARCHAR(255),
        year INT,
        season VARCHAR(10),
        average_value FLOAT
    );
    """

    with engine.connect() as connection:
        connection.execute(text(create_dimension_location_table))
        connection.execute(text(create_dimension_parameter_table))
        connection.execute(text(create_dimension_time_table))
        connection.execute(text(create_fact_table))
        connection.execute(text(create_daily_aggregates_table))
        connection.execute(text(create_seasonal_trends_table))


def save_to_postgresql(df_location, df_parameter, df_time, df_facts, daily_avg, seasonal_avg):
    """
    Save the DataFrames to PostgreSQL.
    """
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
    daily_avg.write.jdbc(url=postgres_url, table="daily_avg_measurements", mode="overwrite", properties=postgres_properties)
    seasonal_avg.write.jdbc(url=postgres_url, table="seasonal_avg_measurements", mode="overwrite", properties=postgres_properties)


def main():
    spark = SparkSession.builder \
        .appName("Air Quality Data Processing") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1,org.postgresql:postgresql:42.2.18") \
        .getOrCreate()

    df = read_data_from_mongodb(spark)
    df.printSchema()

    df_measurements = clean_and_prepare_data(df)
    df_location, df_parameter, df_time = create_dimension_tables(df_measurements)
    df_facts = create_fact_table(df_measurements, df_location, df_parameter, df_time)

    df_daily_avg = calculate_daily_aggregates(df_measurements)
    df_seasonal_avg = calculate_seasonal_trends(df_measurements)

    # postgres_properties = {
    #     "user": "admin",
    #     "password": "pass123",
    #     "driver": "org.postgresql.Driver"
    # }
    # postgres_url = "jdbc:postgresql://localhost:5432/air_quality"

    engine = create_engine(f'postgresql+psycopg2://admin:pass123@localhost:5432/air_quality')
    create_tables_in_postgresql(engine)
    save_to_postgresql(df_location, df_parameter, df_time, df_facts, df_daily_avg, df_seasonal_avg)
    spark.stop()

if __name__ == "__main__":
    main()
