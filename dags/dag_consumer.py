# import sys
# import os
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime, timedelta
# from airflow.operators.dagrun_operator import TriggerDagRunOperator
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
#     'air_quality_consumer',
#     default_args=default_args,
#     description='Consume Kafka messages and store in MongoDB',
#     schedule_interval=timedelta(minutes=5),
# )

# consume_task = PythonOperator(
#     task_id='consume_and_store_data',
#     python_callable=consume_and_store_data,  # Ensure this is the correct function
#     dag=dag,
# )
# trigger_processing_dag = TriggerDagRunOperator(
#     task_id='trigger_processing_dag',
#     trigger_dag_id='air_quality_processing',  # Ensure this DAG ID matches
#     dag=dag,
# )
# consume_task >> trigger_processing_dag
