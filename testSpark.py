from pyspark.sql import SparkSession
from constantes import MONGO_URI, MONGO_DB, MONGO_COLLECTION
from pyspark.sql.functions import col, year, month, dayofmonth, hour, avg, when, monotonically_increasing_id, to_timestamp, max
from sqlalchemy import create_engine, text
from pyspark.sql.window import Window
from pyspark.sql import functions as F

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
    data_frame.show()
    data_frame = data_frame.repartition(100)
    return data_frame


def convert_to_si(parameter_name, value, unit):
    # Dictionnaire des conversions pour chaque paramètre
    conversions = {
        'co': {
            'ppm': lambda x: (x * 1145.94, 'µg/m³'),   # Conversion de ppm à µg/m³
            'ppb': lambda x: (x * 1.14594, 'µg/m³'),    # Conversion de ppb à µg/m³
            'µg/m³': lambda x: (x, 'µg/m³')             # µg/m³ est déjà l'unité SI
        },
        'no2': {
            'ppm': lambda x: (x * 1912.87, 'µg/m³'),  # Conversion de ppm à µg/m³
            'ppb': lambda x: (x * 1.91287, 'µg/m³'),   # Conversion de ppb à µg/m³
            'µg/m³': lambda x: (x, 'µg/m³')             # µg/m³ est déjà l'unité SI
        },
        'so2': {
            'ppm': lambda x: (x * 2620.8, 'µg/m³'),    # Conversion de ppm à µg/m³
            'ppb': lambda x: (x * 2.6208, 'µg/m³'),    # Conversion de ppb à µg/m³
            'µg/m³': lambda x: (x, 'µg/m³')            # µg/m³ est déjà l'unité SI
        },
        'o3': {
            'ppb': lambda x: (x * 2.0, 'µg/m³'),        # Conversion de ppb à µg/m³ (approximation)
            'µg/m³': lambda x: (x, 'µg/m³')             # µg/m³ est déjà l'unité SI
        },
        'pm': {
            'µg/m³': lambda x: (x, 'µg/m³')             # PM est toujours en µg/m³
        },
        'humidity': {
            '%': lambda x: (x, '%')                 # L'humidité reste en pourcentage
        },
        'pressure': {
            'hpa': lambda x: (x * 100,  'Pa'),       # Conversion de hPa à Pa
            'mb': lambda x: (x * 100,  'Pa'),         # Conversion de mb à Pa (1 hPa = 1 mb)
            'pa': lambda x: (x,  'Pa')                # Pa est déjà l'unité SI
        },
        'temperature': {
            'f': lambda x: ((x - 32) * 5/9, '°C'),   # Conversion de °F à °C
            'c': lambda x: (x, '°C'),                # °C est déjà l'unité SI
            'k': lambda x: (x - 273.15, '°C')        # Conversion de K à °C
        }
    }

    # Gestion des paramètres de type PM (PM1, PM10, PM2.5, PM4)
    if parameter_name.startswith('pm'):
        parameter_type = 'pm'
    else:
        parameter_type = parameter_name
    
    # Vérifier si le paramètre et l'unité sont reconnus
    if parameter_type in conversions:
        if unit in conversions[parameter_type]:
            value_converted, new_unit = conversions[parameter_type][unit](value)
            return value_converted, new_unit
        else:
            raise ValueError(f"Unité '{unit}' non reconnue pour le paramètre '{parameter_name}'")
    else:
        raise ValueError(f"Paramètre '{parameter_name}' non reconnu")

def newfonction(test_name):
    """"""
    return True
def convert_units_df(df):
    # Define the conversion logic for each parameter
    df = df.withColumn("value", 
                       F.when((F.col("parameter_name") == "co") & (F.col("unit") == "ppm"), F.col("value") * 1145.94)
                        .when((F.col("parameter_name") == "co") & (F.col("unit") == "ppb"), F.col("value") * 1.14594)
                        .when((F.col("parameter_name") == "no2") & (F.col("unit") == "ppm"), F.col("value") * 1912.87)
                        .when((F.col("parameter_name") == "no2") & (F.col("unit") == "ppb"), F.col("value") * 1.91287)
                        .when((F.col("parameter_name") == "so2") & (F.col("unit") == "ppm"), F.col("value") * 2620.8)
                        .when((F.col("parameter_name") == "so2") & (F.col("unit") == "ppb"), F.col("value") * 2.6208)
                        .when((F.col("parameter_name") == "o3") & (F.col("unit") == "ppb"), F.col("value") * 2.0)
                        .when((F.col("parameter_name").startswith("pm")) & (F.col("unit") == "µg/m³"), F.col("value"))
                        .when((F.col("parameter_name") == "pressure") & (F.col("unit") == "hpa"), F.col("value") * 100)
                        .when((F.col("parameter_name") == "pressure") & (F.col("unit") == "mb"), F.col("value") * 100)
                        .when((F.col("parameter_name") == "pressure") & (F.col("unit") == "pa"), F.col("value"))
                        .when((F.col("parameter_name") == "temperature") & (F.col("unit") == "f"), (F.col("value") - 32) * 5/9)
                        .when((F.col("parameter_name") == "temperature") & (F.col("unit") == "k"), F.col("value") - 273.15)
                        .otherwise(F.col("value")))

    # Update the units accordingly
    df = df.withColumn("unit", 
                       F.when(F.col("parameter_name").isin("co", "no2", "so2", "o3") & F.col("unit").isin("ppm", "ppb"), "µg/m³")
                        .when((F.col("parameter_name") == "pressure") & F.col("unit").isin("hpa", "mb"), "Pa")
                        .when((F.col("parameter_name") == "temperature") & F.col("unit").isin("f", "k", "c"), "°C")
                        .otherwise(F.col("unit")))
    
    return df

def clean_and_prepare_data(df):
    # I have to use the conversion func to return the new value and the new unit and modify those values in my df data. 
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

    # Explode the measurements array to create individual rows for each parameter
    df_exploded = df.withColumn("measurement", F.explode("measurements"))

    # Select and rename columns
    df_measurements = df_exploded.select(
        F.col("_id.oid").alias("measurement_id"),
        F.col("city"),
        F.col("country"),
        F.col("location").alias("location_name"),
        F.col("coordinates.latitude").alias("latitude"),
        F.col("coordinates.longitude").alias("longitude"),
        F.col("measurement.parameter").alias("parameter_name"),
        F.col("measurement.value").alias("value"),
        F.col("measurement.unit").alias("unit"),
        F.col("measurement.lastUpdated").alias("timestamp")
    )
    
    
    # Convert parameter names to lowercase
    df_measurements = df_measurements.withColumn("parameter_name", F.lower(F.col("parameter_name")))

    df_measurements.show()

    # Filtrer pour le paramètre 'pressure'
    df_pressure = df_measurements.filter(col("parameter_name") == "pressure")

    # Trouver la valeur maximale
    max_pressure = df_pressure.agg(max("value").alias("max_value")).collect()[0]["max_value"]

    print(f"La plus grande valeur pour le paramètre 'pressure' est : {max_pressure}")

    df_measurements = convert_units_df(df_measurements)

    # Filtrer les données pour le paramètre 'pressure'
    df_pressure = df_measurements.filter(col("parameter_name") == "pressure")

    # Définir les seuils min et max pour la pression
    min_pressure_threshold = 87000
    max_pressure_threshold = 108400

    # Corriger les valeurs de pression en dehors des seuils
    df_pressure_corrected = df_pressure.withColumn(
        "corrected_value",
        when(col("value") < min_pressure_threshold, min_pressure_threshold)
        .when(col("value") > max_pressure_threshold, max_pressure_threshold)
        .otherwise(col("value"))
    )

    # Joindre les données corrigées sur df_measurements
    df_measurements = df_measurements.join(
        df_pressure_corrected.select("measurement_id", "corrected_value"),
        on="measurement_id",
        how="left"
    )

    # Mettre à jour la colonne "value" pour les lignes où le parameter_name est "pressure"
    df_measurements = df_measurements.withColumn(
        "value",
        when(col("parameter_name") == "pressure", col("corrected_value")).otherwise(col("value"))
    ).drop("corrected_value")
    
    window_spec = Window.partitionBy("measurement_id", "parameter_name", "timestamp").orderBy("value")
    df_measurements = df_measurements.withColumn("row_number", F.row_number().over(window_spec))
    df_measurements = df_measurements.filter(F.col("row_number") == 1).drop("row_number")

    df_measurements = df_measurements.repartition(100)

    return df_measurements

def create_dimension_tables(df_measurements):
    """
    Create dimension tables based on the input dataframe of measurements.

    Args:
    df_measurements (DataFrame): The input dataframe containing measurements.

    Returns:
    DataFrame: df_location - A dataframe with location information.
    DataFrame: df_parameter - A dataframe with parameter information.
    DataFrame: df_time - A dataframe with time information.
    """


    # Create df_location
    df_location = df_measurements.select("location_name", "city", "country", "latitude", "longitude").distinct()
    df_location = df_location.withColumn("location_id", monotonically_increasing_id())
    df_location = df_location.select("location_id", "location_name", "city", "country", "latitude", "longitude")

    # Create df_parameter
    df_parameter = df_measurements.select("parameter_name").distinct()
    df_parameter = df_parameter.withColumn("parameter_id", monotonically_increasing_id())
    df_parameter = df_parameter.select("parameter_id", "parameter_name")

    # Create df_time
    df_time = df_measurements.select("timestamp").distinct() \
        .withColumn("year", year("timestamp")) \
        .withColumn("month", month("timestamp")) \
        .withColumn("day", dayofmonth("timestamp")) \
        .withColumn("hour", hour("timestamp")) \
        .withColumn("time_id", monotonically_increasing_id())
    df_time = df_time.select("time_id", "timestamp", "year", "month", "day", "hour")
    
    df_time = df_time.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ssXXX"))
    # df_time = df_time.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss"))

    
    return df_location, df_parameter, df_time






def create_fact_table(df_measurements, df_location, df_parameter, df_time):
    """
    Create a fact table by joining dimension tables and measurements.
    Args:
    df_measurements (DataFrame): The input dataframe containing measurements.
    df_location (DataFrame): The dataframe containing location dimensions.
    df_parameter (DataFrame): The dataframe containing parameter dimensions.
    df_time (DataFrame): The dataframe containing time dimensions.
    Returns:
    DataFrame: A dataframe representing the fact table.
    """
    # Join df_measurements with dimension tables to get dimension IDs
    df_facts = df_measurements \
        .join(df_location, 
              (df_measurements.latitude == df_location.latitude) & 
              (df_measurements.longitude == df_location.longitude), 
              how='left') \
        .join(df_parameter, df_measurements.parameter_name == df_parameter.parameter_name, how='left') \
        .join(df_time, df_measurements.timestamp == df_time.timestamp, how='left') \
        .select(
            df_measurements.measurement_id,
            df_location.location_id,
            df_parameter.parameter_id,
            df_time.time_id,
            df_measurements.value,
            df_measurements.unit
        )
    
    return df_facts




def calculate_daily_aggregates(df_measurements):
    """
    Calculate daily averages of measurements for each city, country, and parameter.
    
    Args:
        df_measurements (DataFrame): The input DataFrame containing the measurements.

    Returns:
        DataFrame: A DataFrame containing the daily aggregates.
    """
    df_daily = df_measurements.withColumn("date", F.to_date("timestamp"))
    df_daily_aggregates = df_daily.groupBy("city", "country", "parameter_name", "date") \
                                  .agg(F.avg("value").alias("average_value"))

    return df_daily_aggregates

def calculate_seasonal_trends(df):
    # Extract year and month from the timestamp
    df_with_date = df.withColumn("year", year(df["timestamp"])) \
                     .withColumn("month", month(df["timestamp"]))
    
    # Define the season based on the month
    df_with_season = df_with_date.withColumn(
        "season",
        when(df_with_date["month"].isin([12, 1, 2]), "Winter")
        .when(df_with_date["month"].isin([3, 4, 5]), "Spring")
        .when(df_with_date["month"].isin([6, 7, 8]), "Summer")
        .when(df_with_date["month"].isin([9, 10, 11]), "Fall")
    )
    
    # Group by city, country, parameter_name, year, and season, then calculate the average value
    df_seasonal_trends = df_with_season.groupBy("city", "country", "parameter_name", "year", "season") \
                                       .agg(avg("value").alias("average_value"))
    
    return df_seasonal_trends

def create_tables_in_postgresql(engine):
    """
    Create the tables in PostgreSQL using the provided engine.
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
        parameter_name VARCHAR(255)
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
        measurement_id VARCHAR(255) PRIMARY KEY,
        location_id INT,
        time_id INT,
        parameter_id INT,
        value FLOAT,
        unit VARCHAR(255),
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
    daily_avg.write.jdbc(url=postgres_url, table="daily_aggregates", mode="overwrite", properties=postgres_properties)
    seasonal_avg.write.jdbc(url=postgres_url, table="seasonal_trends", mode="overwrite", properties=postgres_properties)

def main():
    spark = SparkSession.builder \
        .appName("Air Quality Data Processing") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1,org.postgresql:postgresql:42.2.18") \
        .getOrCreate()

    df = read_data_from_mongodb(spark)
    # df.printSchema()

    df_measurements = clean_and_prepare_data(df)
    df_measurements.printSchema()
    df_measurements.show()
    df_location, df_parameter, df_time = create_dimension_tables(df_measurements)
    print("Shema df_time")
    df_time.printSchema()
    df_facts = create_fact_table(df_measurements, df_location, df_parameter, df_time)
    daily_avg = calculate_daily_aggregates(df_measurements)
    seasonal_avg = calculate_seasonal_trends(df_measurements)

    engine = create_engine("postgresql://admin:pass123@localhost:5432/air_quality")

    create_tables_in_postgresql(engine)
    save_to_postgresql(df_location, df_parameter, df_time, df_facts, daily_avg, seasonal_avg)

if __name__ == "__main__":
    main()

# ETL Khdam mais pb de conception architecture de BD dans postgreSQL. 
# Dashboard grafana
# Nzid Airflow pour l'automatisation