# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.operators.dagrun_operator import TriggerDagRunOperator
# from airflow.utils.dates import days_ago
# from pyspark.sql import SparkSession
# from sqlalchemy import create_engine
# import sys
# sys.path.append('/opt/airflow/scripts')
# from testSpark import (
#     read_data_from_mongodb,
#     clean_and_prepare_data,
#     create_dimension_tables,
#     create_fact_table,
#     calculate_daily_aggregates,
#     calculate_seasonal_trends,
#     create_tables_in_postgresql,
#     save_to_postgresql
# )

# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': days_ago(1),
#     'retries': 1,
# }

# with DAG(
#     'air_quality_processing',
#     default_args=default_args,
#     description='DAG for processing air quality data with Spark',
#     schedule_interval=None,
# ) as dag:

#     def process_data(**kwargs):
#         spark = SparkSession.builder \
#             .appName("Air Quality Data Processing") \
#             .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1,org.postgresql:postgresql:42.2.18") \
#             .getOrCreate()

#         # Read data from MongoDB
#         df = read_data_from_mongodb(spark)

#         # Clean and prepare data
#         df_measurements = clean_and_prepare_data(df)

#         # Create dimension tables
#         df_location, df_parameter, df_time = create_dimension_tables(df_measurements)

#         # Create fact table
#         df_facts = create_fact_table(df_measurements, df_location, df_parameter, df_time)

#         # Calculate daily aggregates
#         daily_avg = calculate_daily_aggregates(df_measurements)

#         # Calculate seasonal trends
#         seasonal_avg = calculate_seasonal_trends(df_measurements)

#         # Create tables in PostgreSQL
#         engine = create_engine("postgresql://admin:pass123@localhost:5432/air_quality")
#         create_tables_in_postgresql(engine)

#         # Save data to PostgreSQL
#         save_to_postgresql(df_location, df_parameter, df_time, df_facts, daily_avg, seasonal_avg, engine)

#     process_data_task = PythonOperator(
#         task_id='process_data',
#         python_callable=process_data,
#         provide_context=True,
#     )

#     process_data_task
