from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Configurer les chemins vers les scripts
sys.path.append('/opt/airflow/scripts')

from producer import produce_air_quality_data
from consumer import consume_and_store_data
from sparkTransformAndLoad import transform_store_postgreSQL

# Configuration du DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='air_quality_pipeline',
    default_args=default_args,
    description='Pipeline de traitement des données de qualité de l\'air',
    schedule_interval='*/10 * * * *',  # Exécution toutes les 30 minutes
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # Task 1: Exécuter le producteur Kafka
    produce_task = PythonOperator(
        task_id='produce_air_quality_data',
        python_callable=produce_air_quality_data
    )

    # Task 2: Consommer les données de Kafka et les stocker dans MongoDB
    consume_task = PythonOperator(
        task_id='consume_and_store_data',
        python_callable=consume_and_store_data
    )

    # Task 3: Transformer les données avec PySpark et les stocker dans PostgreSQL
    transform_task = PythonOperator(
        task_id='transform_store_postgreSQL',
        python_callable=transform_store_postgreSQL
    )

    # Définir les dépendances entre les tâches
    produce_task >> consume_task >> transform_task