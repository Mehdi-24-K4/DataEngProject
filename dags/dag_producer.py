# import sys
# import os
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.operators.dagrun_operator import TriggerDagRunOperator
# from datetime import datetime, timedelta

# # Add the path where your consumer module is located
# sys.path.append('/opt/airflow/scripts')

# from consumer import consume_and_store_data  # Import your function from consumer.py

# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': datetime(2024, 8, 23),
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }

# dag = DAG(
#     'air_quality_producer',
#     default_args=default_args,
#     description='Fetch air quality data and send to Kafka',
#     schedule_interval=timedelta(minutes=5),
# )

# produce_task = PythonOperator(
#     task_id='produce_air_quality_data',
#     python_callable=consume_and_store_data,  # Ensure this is the correct function
#     dag=dag,
# )

# trigger_consumer_dag = TriggerDagRunOperator(
#     task_id='trigger_consumer_dag',
#     trigger_dag_id='air_quality_consumer',  # Ensure this DAG ID matches
#     dag=dag,
# )

# produce_task >> trigger_consumer_dag
