from pyspark.sql import SparkSession
from constantes import MONGO_URI, MONGO_DB, MONGO_COLLECTION
from pyspark.sql.functions import col, year, month, dayofmonth, hour, minute, second, avg, when, monotonically_increasing_id, to_timestamp, max, broadcast, from_utc_timestamp, date_format, lit, create_map
from sqlalchemy import create_engine, text
from pyspark.sql.window import Window
from pyspark.sql import functions as F, DataFrame
from pymongo import MongoClient
import pandas as pd
from itertools import chain
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

def mark_data_as_processed():
    """
    Updates the 'processed' field to True for all documents in MongoDB.
    """
    try:
        # Étape 3 : Se connecter à MongoDB via pymongo
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DB]
        collection = db[MONGO_COLLECTION]
        
        # Log avant la mise à jour pour voir combien de documents ne sont pas encore traités
        unprocessed_count = collection.count_documents({"processed": {"$ne": False}})
        print(f"Documents not marked as processed: {unprocessed_count}")

        # Mise à jour de tous les documents dans la collection
        result = collection.update_many({"processed": {"$ne": True}}, {"$set": {"processed": True}})
        
        # Log après la mise à jour
        print(f"Marked {result.modified_count} documents as processed in MongoDB.")


    except Exception as e:
        print(f"Error marking data as processed: {e}")

    finally:
        # Assure la fermeture de la connexion
        client.close()

    
def read_data_from_mongodb(spark_session):
    """
    Reads data from MongoDB using the provided Spark session and returns a DataFrame.

    Parameters:
        spark_session (SparkSession): The Spark session used to read data from MongoDB.

    Returns:
        DataFrame: A DataFrame containing the data read from MongoDB.
    """
    try:
        # Définir l'URI MongoDB
        mongo_uri = f"{MONGO_URI}{MONGO_DB}.{MONGO_COLLECTION}"
        
        # Lire les données depuis MongoDB
        data_frame = spark_session.read.format("com.mongodb.spark.sql.DefaultSource") \
            .option("uri", mongo_uri) \
            .load()
        
        # Filtrer pour obtenir uniquement les données non traitées
        df = data_frame.filter(col("processed") == False)
        data_frame2 = data_frame.filter(col("processed") == True)
        # Afficher un échantillon des données pour vérification
        print("data_frame2 for processed data")
        data_frame2.show()
        print("data_frame for unprocessed data")
        df.show()
        # Repartir les données pour une meilleure performance si nécessaire
        df = df.repartition(10)

        return df

    except Exception as e:
        print(f"Error reading data from MongoDB: {e}")
        raise e

def convert_units_df(df):
    """
    Converts units in the DataFrame based on the parameter name and unit.
    """
    df = df.withColumn("value", 
                       F.when((F.col("parameter_name") == "co") & (F.col("unit") == "ppm"), F.col("value") * 1145.94)
                        .when((F.col("parameter_name") == "co") & (F.col("unit") == "ppb"), F.col("value") * 1.14594)
                        .when((F.col("parameter_name") == "no2") & (F.col("unit") == "ppm"), F.col("value") * 1912.87)
                        .when((F.col("parameter_name") == "no2") & (F.col("unit") == "ppb"), F.col("value") * 1.91287)
                        .when((F.col("parameter_name") == "so2") & (F.col("unit") == "ppm"), F.col("value") * 2620.8)
                        .when((F.col("parameter_name") == "so2") & (F.col("unit") == "ppb"), F.col("value") * 2.6208)
                        .when((F.col("parameter_name") == "o3") & (F.col("unit") == "ppb"), F.col("value") * 2.0)
                        .when((F.col("parameter_name").startswith("pm")) & (F.col("unit") == "µg/m³"), F.col("value"))
                        .when((F.col("parameter_name") == "no") & (F.col("unit") == "ppm"), F.col("value") * 1230)  # Conversion approximative pour NO
                        .when((F.col("parameter_name") == "no") & (F.col("unit") == "ppb"), F.col("value") * 1.23)  # Conversion approximative pour NO
                        .when((F.col("parameter_name") == "ch4") & (F.col("unit") == "ppm"), F.col("value") * 655.66)  # Conversion approximative pour CH4
                        .when((F.col("parameter_name") == "bc"), F.col("value"))  # Pas de conversion pour BC, généralement en µg/m³
                        .when((F.col("parameter_name") == "pressure") & (F.col("unit") == "hpa"), F.col("value") * 100)
                        .when((F.col("parameter_name") == "pressure") & (F.col("unit") == "mb"), F.col("value") * 100)
                        .when((F.col("parameter_name") == "pressure") & (F.col("unit") == "pa"), F.col("value"))
                        .when((F.col("parameter_name") == "temperature") & (F.col("unit") == "f"), (F.col("value") - 32) * 5/9)
                        .when((F.col("parameter_name") == "temperature") & (F.col("unit") == "k"), F.col("value") - 273.15)
                        .otherwise(F.col("value")))

    df = df.withColumn("unit", 
                       F.when(F.col("parameter_name").isin("co", "no2", "so2", "o3", "no", "ch4") & F.col("unit").isin("ppm", "ppb"), "µg/m³")
                        .when(F.col("parameter_name").isin("pm25", "pm4", "pm10", "bc"), "µg/m³")
                        .when((F.col("parameter_name") == "pressure") & F.col("unit").isin("hpa", "mb"), "Pa")
                        .when((F.col("parameter_name") == "temperature") & F.col("unit").isin("f", "k", "c"), "°C")
                        .otherwise(F.col("unit")))
    
    return df

def clean_and_prepare_data(df):
    """
    Clean and prepare data for processing.
    Args:
        df (DataFrame): The input DataFrame containing the raw data.
    Returns:
        DataFrame: The cleaned and prepared DataFrame.
    """
    df_exploded = df.withColumn("measurement", F.explode("measurements"))

    df_measurements = df_exploded.select(
        F.concat(F.col("_id.oid"), F.lit("_"), F.col("measurement.parameter")).alias("measurement_id"),
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
    # Conversion du timestamp en UTC+2
    df_measurements = df_measurements.withColumn(
        "timestamp",
        date_format(
            from_utc_timestamp(F.col("timestamp"), "Europe/Paris"),
            "yyyy-MM-dd'T'HH:mm:ssXXX"
        )
    )
    df_measurements = df_measurements.withColumn("parameter_name", F.lower(F.col("parameter_name")))

    df_pressure = df_measurements.filter(col("parameter_name") == "pressure")
    max_pressure = df_pressure.agg(max("value").alias("max_value")).collect()[0]["max_value"]
    print(f"La plus grande valeur pour le paramètre 'pressure' est : {max_pressure}")

    df_measurements = convert_units_df(df_measurements)

    df_pressure = df_measurements.filter(col("parameter_name") == "pressure")
    min_pressure_threshold = 87000
    max_pressure_threshold = 108400

    df_pressure_corrected = df_pressure.withColumn(
        "corrected_value",
        when(col("value") < min_pressure_threshold, min_pressure_threshold)
        .when(col("value") > max_pressure_threshold, max_pressure_threshold)
        .otherwise(col("value"))
    )

    df_measurements = df_measurements.join(
        df_pressure_corrected.select("measurement_id", "corrected_value"),
        on="measurement_id",
        how="left"
    )

    df_measurements = df_measurements.withColumn(
        "value",
        when(col("parameter_name") == "pressure", col("corrected_value")).otherwise(col("value"))
    ).drop("corrected_value")
    
    window_spec = Window.partitionBy("measurement_id", "parameter_name", "timestamp").orderBy("value")
    df_measurements = df_measurements.withColumn("row_number", F.row_number().over(window_spec))
    df_measurements = df_measurements.filter(F.col("row_number") == 1).drop("row_number")

    df_measurements = df_measurements.repartition(10)

    return df_measurements

def create_dimension_tables(df_measurements, engine):
    """
    Create dimension tables based on the input dataframe of measurements, with handling of primary key incrementation.
    Args:
    df_measurements (DataFrame): The input dataframe containing measurements.
    engine (Engine): SQLAlchemy engine for PostgreSQL.
    Returns:
    DataFrame: df_location, df_parameter, df_time
    """
    
    def get_next_id(conn, table_name, id_column):
        result = conn.execute(text(f"SELECT MAX({id_column}) FROM {table_name}"))
        max_id = result.scalar()
        return (max_id + 1) if max_id is not None else 1

    with engine.connect() as conn:
        next_location_id = get_next_id(conn, 'dimension_location', 'location_id')
        next_parameter_id = get_next_id(conn, 'dimension_parameter', 'parameter_id')
        next_time_id = get_next_id(conn, 'dimension_time', 'time_id')

    df_location = df_measurements.select("location_name", "city", "country", "latitude", "longitude").distinct()
    df_location = df_location.withColumn("location_id", F.monotonically_increasing_id() + next_location_id)
    df_location = df_location.select("location_id", "location_name", "city", "country", "latitude", "longitude")
    # Supprime les doublons basés sur latitude et longitude
    df_location = df_location.dropDuplicates(["latitude", "longitude"])


    df_parameter = df_measurements.select("parameter_name").distinct()
    df_parameter = df_parameter.withColumn("parameter_id", F.monotonically_increasing_id() + next_parameter_id)
    df_parameter = df_parameter.select("parameter_id", "parameter_name")

    # Assurez-vous que le timestamp est déjà au bon format (UTC+2)
    df_time = df_measurements.select("timestamp").distinct()
    
    # Pas besoin de convertir le timestamp ici car il a déjà été converti dans clean_and_prepare_data
    df_time = df_time \
        .withColumn("year", year("timestamp")) \
        .withColumn("month", month("timestamp")) \
        .withColumn("day", dayofmonth("timestamp")) \
        .withColumn("hour", hour("timestamp")) \
        .withColumn("minute", minute(col("timestamp"))) \
        .withColumn("second", second(col("timestamp"))) \
        .withColumn("time_id", F.monotonically_increasing_id() + next_time_id)

    df_time = df_time.select("time_id", "timestamp", "year", "month", "day", "hour", "minute", "second")

    #New for optimization
    df_location = df_location.select("location_id", "location_name", "city", "country", "latitude", "longitude").coalesce(1)
    df_parameter = df_parameter.select("parameter_id", "parameter_name").coalesce(1)
    df_time = df_time.select("time_id", "timestamp", "year", "month", "day", "hour", "minute", "second").coalesce(1)

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
    df_facts = df_measurements \
        .join(broadcast(df_location), 
              (df_measurements.latitude == df_location.latitude) & 
              (df_measurements.longitude == df_location.longitude), 
              how='left') \
        .join(broadcast(df_parameter), df_measurements.parameter_name == df_parameter.parameter_name, how='left') \
        .join(broadcast(df_time), df_measurements.timestamp == df_time.timestamp, how='left') \
        .select(
            df_measurements.measurement_id,
            df_location.location_id,
            df_parameter.parameter_id,
            df_time.time_id,
            df_measurements.value,
            df_measurements.unit
        )
    
    return df_facts

def filter_and_reassign_ids(df_location: DataFrame, df_parameter: DataFrame, df_time: DataFrame, engine):
    """
    Filter duplicates from dimension DataFrames based on existing records in the database and reassign primary keys.
    Args:
    df_location (DataFrame): DataFrame containing location data.
    df_parameter (DataFrame): DataFrame containing parameter data.
    df_time (DataFrame): DataFrame containing time data.
    engine (Engine): SQLAlchemy engine for PostgreSQL.
    Returns:
    DataFrame: Filtered and ID reassigned df_location, df_parameter, df_time
    """

    def get_existing_values(conn, table_name, columns):
        query = f"SELECT {', '.join(columns)} FROM {table_name}"
        result = conn.execute(text(query))
        return set(tuple(row) for row in result)
    
    def get_next_id(conn, table_name, id_column):
        result = conn.execute(text(f"SELECT MAX({id_column}) FROM {table_name}"))
        max_id = result.scalar()
        return (max_id + 1) if max_id is not None else 1

    with engine.connect() as conn:
        existing_locations = get_existing_values(conn, 'dimension_location', ['latitude', 'longitude'])
        existing_parameters = get_existing_values(conn, 'dimension_parameter', ['parameter_name'])
        existing_times = get_existing_values(conn, 'dimension_time', ['timestamp'])

        next_location_id = get_next_id(conn, 'dimension_location', 'location_id')
        next_parameter_id = get_next_id(conn, 'dimension_parameter', 'parameter_id')
        next_time_id = get_next_id(conn, 'dimension_time', 'time_id')

    # Convert tuples to lists of scalars for comparison
    existing_lat_long = [f"{lat}_{long}" for lat, long in existing_locations]
    existing_parameters = [param for (param,) in existing_parameters]
    existing_times = [ts for (ts,) in existing_times]

    # Filtrage des doublons pour df_location
    df_location = df_location.withColumn("lat_long", F.concat_ws("_", F.col("latitude"), F.col("longitude")))
    df_location = df_location.filter(~F.col("lat_long").isin(existing_lat_long))
    df_location = df_location.drop("lat_long")

    # Réassignation des identifiants pour df_location
    if not df_location.rdd.isEmpty():
        window_location = Window.orderBy(F.monotonically_increasing_id())
        df_location = df_location.withColumn("location_id", F.row_number().over(window_location) + (next_location_id - 1))

    
    # Filtrage des doublons pour df_parameter
    df_parameter = df_parameter.filter(~F.col("parameter_name").isin(existing_parameters))

    # Réassignation des identifiants pour df_parameter
    if not df_parameter.rdd.isEmpty():
        window_parameter = Window.orderBy(F.monotonically_increasing_id())
        df_parameter = df_parameter.withColumn("parameter_id", F.row_number().over(window_parameter) + (next_parameter_id - 1))


    # Filtrage des doublons pour df_time
    df_time = df_time.filter(~F.col("timestamp").isin(existing_times))


    # Réassignation des identifiants pour df_time
    if not df_time.rdd.isEmpty():
        window_time = Window.orderBy(F.monotonically_increasing_id())
        df_time = df_time.withColumn("time_id", F.row_number().over(window_time) + (next_time_id - 1))

    return df_location, df_parameter, df_time

def create_tables_in_postgresql(engine):
    """
    Create necessary tables in PostgreSQL database.
    Args:
    engine (Engine): SQLAlchemy engine for PostgreSQL.
    """
    with engine.connect() as connection:
        connection.execute(text("""
        CREATE TABLE IF NOT EXISTS dimension_location (
            location_id BIGINT PRIMARY KEY,
            location_name VARCHAR,
            city VARCHAR,
            country VARCHAR,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            UNIQUE (latitude, longitude)
        );
        """))
        connection.execute(text("""
        CREATE TABLE IF NOT EXISTS dimension_parameter (
            parameter_id BIGINT PRIMARY KEY,
            parameter_name VARCHAR,
            UNIQUE (parameter_name)
        );
        """))
        connection.execute(text("""
        CREATE TABLE IF NOT EXISTS dimension_time (
            time_id BIGINT PRIMARY KEY,
            timestamp TIMESTAMP,
            year INT,
            month INT,
            day INT,
            hour INT,
            minute INT,
            second INT,
            UNIQUE (timestamp)
        );
        """))
        connection.execute(text("""
        CREATE TABLE IF NOT EXISTS air_quality_measurements (
            measurement_id VARCHAR PRIMARY KEY,
            location_id BIGINT,
            parameter_id BIGINT,
            time_id BIGINT,
            value DOUBLE PRECISION,
            unit VARCHAR,
            FOREIGN KEY (location_id) REFERENCES dimension_location(location_id),
            FOREIGN KEY (parameter_id) REFERENCES dimension_parameter(parameter_id),
            FOREIGN KEY (time_id) REFERENCES dimension_time(time_id)
        );
        """))
        connection.execute(text("""
        CREATE TABLE IF NOT EXISTS daily_aggregates (
            date DATE,
            parameter_name VARCHAR,
            daily_avg DOUBLE PRECISION
        );
        """))
        connection.execute(text("""
        CREATE TABLE IF NOT EXISTS seasonal_trends (
            season VARCHAR,
            parameter_name VARCHAR,
            seasonal_avg DOUBLE PRECISION
        );
        """))

        # Create indexes if they don't exist
        connection.execute(text("""
        CREATE INDEX IF NOT EXISTS idx_dimension_location_location_name ON dimension_location(location_name);
        """))
        connection.execute(text("""
        CREATE INDEX IF NOT EXISTS idx_dimension_parameter_parameter_name ON dimension_parameter(parameter_name);
        """))
        connection.execute(text("""
        CREATE INDEX IF NOT EXISTS idx_dimension_time_timestamp ON dimension_time(timestamp);
        """))
        connection.execute(text("""
        CREATE INDEX IF NOT EXISTS idx_air_quality_measurements_location_id ON air_quality_measurements(location_id);
        """))
        connection.execute(text("""
        CREATE INDEX IF NOT EXISTS idx_air_quality_measurements_parameter_id ON air_quality_measurements(parameter_id);
        """))
        connection.execute(text("""
        CREATE INDEX IF NOT EXISTS idx_air_quality_measurements_time_id ON air_quality_measurements(time_id);
        """))
        connection.execute(text("""
        CREATE INDEX IF NOT EXISTS idx_daily_aggregates_date ON daily_aggregates(date);
        """))
        connection.execute(text("""
        CREATE INDEX IF NOT EXISTS idx_daily_aggregates_parameter_name ON daily_aggregates(parameter_name);
        """))
        connection.execute(text("""
        CREATE INDEX IF NOT EXISTS idx_seasonal_trends_season ON seasonal_trends(season);
        """))
        connection.execute(text("""
        CREATE INDEX IF NOT EXISTS idx_seasonal_trends_parameter_name ON seasonal_trends(parameter_name);
        """))

def delete_existed_dimension(df_location, df_time, df_parameter, engine, spark):
    """
    Delete existing dimensions from the database and filter out existing data from the input DataFrames.

    Parameters:
    - df_location (DataFrame): Input location DataFrame
    - df_time (DataFrame): Input time DataFrame
    - df_parameter (DataFrame): Input parameter DataFrame
    - engine (Engine): Database engine object
    - spark (SparkSession): Spark session object

    Returns:
    - df_location_filtered (DataFrame): Filtered location DataFrame with non-existing data
    - df_time_filtered (DataFrame): Filtered time DataFrame with non-existing data
    - df_parameter_filtered (DataFrame): Filtered parameter DataFrame with non-existing data

    Notes:
    - The function retrieves existing data from the database using the provided engine.
    - It creates Spark DataFrames from the existing data and persists them for efficient reuse.
    - The function filters out existing data from the input DataFrames using left anti joins.
    - Optional debug prints show the number of rows before and after filtering.
    - Finally, the function unpersists the created DataFrames to free up resources.
    """
    # try:
    # Récupérer les données existantes de la base de données
    with engine.connect() as conn:
        existing_locations = pd.read_sql(f'select location_id from dimension_location', conn)
        existing_parameters = pd.read_sql(f'select parameter_id from dimension_parameter', conn)
        existing_times = pd.read_sql(f'select time_id from dimension_time', conn)

    # Définir les schémas pour les DataFrames Spark
    location_schema = StructType([StructField('location_id', IntegerType(), True)])
    parameter_schema = StructType([StructField('parameter_id', IntegerType(), True)])
    time_schema = StructType([StructField('time_id', IntegerType(), True)])
    

    # Créer les DataFrames Spark à partir des données existantes
    locations = spark.createDataFrame(existing_locations, location_schema)
    parameters = spark.createDataFrame(existing_parameters, parameter_schema)
    times = spark.createDataFrame(existing_times, time_schema)

    # Filtrer les lignes de df_location, df_parameter et df_time
    df_location_filtered = df_location.join(locations, on="location_id", how="left_anti")
    df_parameter_filtered = df_parameter.join(parameters, on="parameter_id", how="left_anti")
    df_time_filtered = df_time.join(times, on="time_id", how="left_anti")

    # Optionnel : Afficher le nombre de lignes avant et après le filtrage pour le débogage
    print(f"Nombre de lignes avant filtrage (location): {df_location.count()}, après filtrage: {df_location_filtered.count()}")
    print(f"Nombre de lignes avant filtrage (parameter): {df_parameter.count()}, après filtrage: {df_parameter_filtered.count()}")
    print(f"Nombre de lignes avant filtrage (time): {df_time.count()}, après filtrage: {df_time_filtered.count()}")

    return df_location_filtered, df_time_filtered, df_parameter_filtered

def save_to_postgresql(df_location, df_parameter, df_time, df_facts, engine):
    """
    Save data to PostgreSQL database with row-level locks.
    """ 
    #Merge dataframe with table
    df_location_pd = df_location.toPandas() if not df_location.rdd.isEmpty() else pd.DataFrame()
    df_parameter_pd = df_parameter.toPandas() if not df_parameter.rdd.isEmpty() else pd.DataFrame()
    df_time_pd = df_time.toPandas() if not df_time.rdd.isEmpty() else pd.DataFrame()
    df_facts_pd = df_facts.toPandas() if not df_facts.rdd.isEmpty() else pd.DataFrame()

    with engine.connect() as conn:
        trans = conn.begin()  # Démarre une transaction
        try:
            # Insertion des dimensions
            if not df_location_pd.empty:
                df_location_pd.to_sql('dimension_location', conn, if_exists='append', index=False)

            if not df_parameter_pd.empty:
                df_parameter_pd.to_sql('dimension_parameter', conn, if_exists='append', index=False)

            if not df_time_pd.empty:
                df_time_pd.to_sql('dimension_time', conn, if_exists='append', index=False)

            # Insertion des faits avec verrouillage des lignes spécifiques
            if not df_facts_pd.empty:
                # Pour chaque mesure, verrouillez la ligne avant de l'insérer ou la mettre à jour
                for index, row in df_facts_pd.iterrows():
                    conn.execute(text("""
                        SELECT * FROM air_quality_measurements 
                        WHERE measurement_id = :measurement_id FOR UPDATE
                    """), {'measurement_id': row['measurement_id']})
                    
                    conn.execute(text("""
                        INSERT INTO air_quality_measurements (measurement_id, location_id, parameter_id, time_id, value, unit)
                        VALUES (:measurement_id, :location_id, :parameter_id, :time_id, :value, :unit)
                        ON CONFLICT (measurement_id) 
                        DO UPDATE SET 
                            location_id = EXCLUDED.location_id,
                            parameter_id = EXCLUDED.parameter_id,
                            time_id = EXCLUDED.time_id,
                            value = EXCLUDED.value,
                            unit = EXCLUDED.unit
                    """), {
                        'measurement_id': row['measurement_id'],
                        'location_id': row['location_id'],
                        'parameter_id': row['parameter_id'],
                        'time_id': row['time_id'],
                        'value': row['value'],
                        'unit': row['unit']
                    })

            trans.commit()  # Valide la transaction
        except Exception as e:
            trans.rollback()  # Annule la transaction en cas d'erreur
            print(f"Error saving to PostgreSQL: {e}")
            raise e

def update_dimension_ids(df, engine, table_name, key_columns, id_column, spark):
    """
    Update dimension IDs by generating new IDs for rows that do not have a matching key in the existing database table.

    This function retrieves existing IDs from a specified database table, compares them with the current DataFrame `df`,
    and generates new IDs for entries in `df` that do not already exist in the database. It supports different key columns 
    (timestamps, latitude/longitude, or other strings) and returns a DataFrame with updated IDs.

    Args:
        df (pyspark.sql.DataFrame): The DataFrame containing the new data with columns that need IDs.
        engine (sqlalchemy.engine.Engine): SQLAlchemy engine object for connecting to the PostgreSQL database.
        table_name (str): The name of the table in the PostgreSQL database from which existing IDs are retrieved.
        key_columns (list of str): List of column names in `df` used to match existing records. Can be timestamps, latitude/longitude, or other strings.
        id_column (str): The name of the column in the database table that stores IDs.
        spark (pyspark.sql.SparkSession): Spark session object used to create DataFrames.

    Returns:
        pyspark.sql.DataFrame: The DataFrame `df` with updated IDs. New IDs are generated for rows that do not have an existing match,
        and the `id_column` is updated accordingly.

    Example:
        df = spark.createDataFrame([...], ["latitude", "longitude", "some_value"])
        updated_df = update_dimension_ids(df, engine, 'dimension_location', ['latitude', 'longitude'], 'location_id', spark)

    Notes:
        - The function assumes that the key columns used for joining are either timestamps, latitude/longitude, or general strings.
        - It generates new IDs by incrementing from the maximum existing ID in the database table.
        - The returned DataFrame will have the same columns as `df`, with `id_column` replaced by updated IDs.

    Raises:
        Exception: If any error occurs during database connection or DataFrame operations.
    """
    # Récupérer les ID existants de la base de données
    with engine.connect() as conn:
        existing_data = pd.read_sql(f"SELECT {', '.join(key_columns)}, {id_column} FROM {table_name}", conn)
        # print('existing_data')
        # print(existing_data.head())

    if key_columns==['timestamp']:
        # Define the schema for the existing DataFrame
        schema = StructType([
            StructField(col, TimestampType(), True) for col in key_columns
        ] + [StructField(id_column, IntegerType(), True)])
    elif(key_columns==['latitude', 'longitude']):
        # Define the schema for the existing DataFrame
        schema = StructType([
            StructField(col, DoubleType(), True) for col in key_columns
        ] + [StructField(id_column, IntegerType(), True)])
    else:
        # Define the schema for the existing DataFrame
        schema = StructType([
            StructField(col, StringType(), True) for col in key_columns
        ] + [StructField(id_column, IntegerType(), True)])

    # Créer un DataFrame Spark à partir des données existantes
    existing_df = spark.createDataFrame(existing_data, schema)

    # Renommer la colonne id dans existing_df pour éviter l'ambiguïté
    existing_df = existing_df.withColumnRenamed(id_column, f"existing_{id_column}")

    # Joindre les données existantes avec le DataFrame actuel
    joined_df = df.join(existing_df, key_columns, "left_outer")

    # Pour les nouvelles entrées, générer de nouveaux ID
    max_id = existing_data[id_column].max() if not existing_data.empty else 0
    joined_df = joined_df.withColumn(
        "new_" + id_column,
        F.when(F.col(f"existing_{id_column}").isNull(), F.monotonically_increasing_id() + F.lit(max_id + 1))
            .otherwise(F.col(f"existing_{id_column}"))
    )

    # Sélectionner les colonnes finales
    final_columns = [col for col in df.columns if col != id_column] + ["new_" + id_column]
    final_df = joined_df.select(*final_columns)

    # Renommer la nouvelle colonne d'ID
    final_df = final_df.withColumnRenamed("new_" + id_column, id_column)
    
    return final_df

def create_mapping_dict(df, key_cols, value_col):
    """
    Create a dictionary mapping from key columns to a value column in a DataFrame.

    Parameters:
    - df (DataFrame): Input DataFrame
    - key_cols (str or list of str): One or more column names to use as keys
    - value_col (str): Column name to use as values

    Returns:
    - dict: A dictionary with keys constructed from the key_cols and values from the value_col

    Notes:
    - If key_cols is a single string, it is converted to a list.
    - The function collects only the necessary columns from the DataFrame.
    - The resulting dictionary has a minimal conversion from the DataFrame data.
    """
    # Assurez-vous que key_cols est toujours une liste
    if not isinstance(key_cols, list):
        key_cols = [key_cols]
    
    # Collecte uniquement les colonnes nécessaires
    data = df.select(key_cols + [value_col]).collect()
    
    # Création du dictionnaire avec une conversion minimale
    if len(key_cols) > 1:
        return {tuple(row[key] for key in key_cols): row[value_col] for row in data}
    else:
        return {row[key_cols[0]]: row[value_col] for row in data}

def create_fact_table_with_mappings(df_measurements, location_map, parameter_map, time_map, spark):
    """
    Create a fact table by joining measurements with location, parameter, and time mappings.

    Parameters:
    - df_measurements (DataFrame): Input measurements DataFrame
    - location_map (dict): Mapping of location coordinates to location IDs
    - parameter_map (dict): Mapping of parameter names to parameter IDs
    - time_map (dict): Mapping of time strings to time IDs
    - spark (SparkSession): Spark session object

    Returns:
    - df_facts (DataFrame): Fact table with location, parameter, and time IDs

    Notes:
    - The function converts the location map to a DataFrame and joins it with the measurements DataFrame.
    - The parameter and time maps are converted to Spark map expressions and applied to the joined DataFrame.
    - The resulting fact table is coalesced to a specified number of partitions.
    """
    # Convertir location_map en DataFrame
    location_map_df = spark.createDataFrame(
        [(float(lat), float(lon), id) for (lat, lon), id in location_map.items()],
        ["latitude", "longitude", "location_id"]
    )
    # Convertir parameter_map et time_map en expressions de map Spark
    parameter_map_expr = create_map([lit(x) for x in chain(*parameter_map.items())])
    time_map_expr = create_map([lit(x) for x in chain(*time_map.items())])

    # Joindre df_measurements avec location_map_df
    df_with_location_id = df_measurements.join(
        location_map_df,
        (df_measurements.latitude == location_map_df.latitude) & 
        (df_measurements.longitude == location_map_df.longitude),
        "left"
    )
    # Appliquer les mappings
    df_facts = df_with_location_id \
        .withColumn("parameter_id", parameter_map_expr[col("parameter_name")]) \
        .withColumn("time_id", time_map_expr[date_format(col("timestamp"), "yyyy-MM-dd HH:mm:ss")]) \
        .select("measurement_id", "location_id", "parameter_id", "time_id", "value", "unit")
    
    # Utilisez coalesce ici, avec un nombre approprié de partitions
    return df_facts.coalesce(5)  # Ajustez le nombre en fonction de votre volume de données


# Main function
def transform_store_postgreSQL():
    spark = SparkSession.builder \
        .appName("Air Quality Data Processing") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1,org.postgresql:postgresql:42.2.18") \
        .config("spark.network.timeout", "800s") \
        .config("spark.executor.heartbeatInterval", "60s") \
        .getOrCreate()
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    try:

        df = read_data_from_mongodb(spark)
        if not df.rdd.isEmpty():
            engine = create_engine("postgresql+psycopg2://admin:pass123@postgres:5432/air_quality")
            create_tables_in_postgresql(engine)
            df_measurements = clean_and_prepare_data(df)
            # df_measurements.show(5)
            df_location, df_parameter, df_time = create_dimension_tables(df_measurements, engine)

            if df_time.isEmpty():
                print("df_time est vide ici.")  

            # Récupérer les ID existants et attribuer de nouveaux ID aux nouvelles dimensions
            df_location = update_dimension_ids(df_location, engine, 'dimension_location', ['latitude', 'longitude'], 'location_id', spark)
            df_parameter = update_dimension_ids(df_parameter, engine, 'dimension_parameter', ['parameter_name'], 'parameter_id', spark)

            df_time = df_time.withColumn("timestamp", col("timestamp").cast(TimestampType()))

            df_time = update_dimension_ids(df_time, engine, 'dimension_time', ['timestamp'], 'time_id', spark)
            if df_time.rdd.isEmpty():
                print("df_time est vide ici.")
            # Créer des dictionnaires de mappage pour chaque dimension
            try:
                location_map = create_mapping_dict(df_location, ["latitude", "longitude"], "location_id")
                print("Location map created successfully")
            except Exception as e:
                print("Error in creating location_map:", e)
            
            try:
                parameter_map = create_mapping_dict(df_parameter, "parameter_name", "parameter_id")
                print("Parameter map created successfully")
            except Exception as e:
                print("Error in creating parameter_map:", e)
            if df_time.isEmpty():
                print("df_time est vide ici.")
            try:
                df_time = df_time.withColumn("timestamp_str", F.date_format(F.col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
                time_map = create_mapping_dict(df_time, "timestamp_str", "time_id")
                df_time = df_time.drop("timestamp_str")
                print("Time map created successfully")
            except Exception as e:
                print(f"Error in creating time_map:", e)


            # Créer la table de faits en utilisant les mappings
            df_facts = create_fact_table_with_mappings(df_measurements, location_map, parameter_map, time_map, spark)
            df_facts = df_facts.coalesce(5)
            try:
                df_location, df_time, df_parameter = delete_existed_dimension(df_location, df_time, df_parameter, engine, spark)
            except Exception as e:
                print(f"Erreur de requête de la base de données :", e)
            try:
                save_to_postgresql(df_location, df_parameter, df_time, df_facts, engine)
                # mark_data_as_processed()
            except Exception as e:
                print(f"Error saving to PostgreSQL: {e}")
                raise e
            try:
                mark_data_as_processed()
            except Exception as e:
                print(f"Error marking data in MongoDB: {e}")
                raise e
            finally:
                print("fermeture de l'engin")
                engine.dispose()
        else:
            print("No new data from the API.")        
    finally:
        spark.catalog.clearCache()
        spark.stop()
        print("Spark session expired")