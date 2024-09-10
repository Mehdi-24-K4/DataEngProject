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
    schedule_interval=timedelta(days=1),
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


#Logs for last task for writing in postgres for second pipeline run
# [2024-09-02, 18:58:11 UTC] {logging_mixin.py:151} INFO - Data marked as processed. Documents updated: 100
# [2024-09-02, 18:58:25 UTC] {logging_mixin.py:151} INFO - Error saving to PostgreSQL: (psycopg2.errors.UniqueViolation) duplicate key value violates unique constraint "dimension_time_timestamp_key"
# DETAIL:  Key ("timestamp")=(2024-03-10 19:29:00+00) already exists.
# [SQL: INSERT INTO dimension_time (time_id, timestamp, year, month, day, hour) VALUES (%(time_id)s, %(timestamp)s, %(year)s, %(month)s, %(day)s, %(hour)s)]
# [parameters: ({'time_id': 29, 'timestamp': '2024-09-02T20:38:00Z', 'year': 2024, 'month': 9, 'day': 2, 'hour': 20}, {'time_id': 30, 'timestamp': '2024-03-10T19:29:00Z', 'year': 2024, 'month': 3, 'day': 10, 'hour': 19}, {'time_id': 31, 'timestamp': '2024-09-03T05:00:00Z', 'year': 2024, 'month': 9, 'day': 3, 'hour': 5}, {'time_id': 32, 'timestamp': '2024-03-10T19:30:00Z', 'year': 2024, 'month': 3, 'day': 10, 'hour': 19}, {'time_id': 33, 'timestamp': '2024-03-10T19:31:00Z', 'year': 2024, 'month': 3, 'day': 10, 'hour': 19}, {'time_id': 34, 'timestamp': '2024-09-02T20:43:00Z', 'year': 2024, 'month': 9, 'day': 2, 'hour': 20}, {'time_id': 35, 'timestamp': '2024-09-03T15:50:23Z', 'year': 2024, 'month': 9, 'day': 3, 'hour': 15}, {'time_id': 36, 'timestamp': '2024-09-03T15:54:06Z', 'year': 2024, 'month': 9, 'day': 3, 'hour': 15}  ... displaying 10 of 27 total bound parameter sets ...  {'time_id': 54, 'timestamp': '2024-09-02T20:23:00Z', 'year': 2024, 'month': 9, 'day': 2, 'hour': 20}, {'time_id': 55, 'timestamp': '2024-09-02T22:44:06Z', 'year': 2024, 'month': 9, 'day': 2, 'hour': 22})]
# (Background on this error at: https://sqlalche.me/e/14/gkpj)
# [2024-09-02, 18:58:25 UTC] {logging_mixin.py:151} INFO - Error saving to PostgreSQL: (psycopg2.errors.UniqueViolation) duplicate key value violates unique constraint "dimension_time_timestamp_key"
# DETAIL:  Key ("timestamp")=(2024-03-10 19:29:00+00) already exists.
# [SQL: INSERT INTO dimension_time (time_id, timestamp, year, month, day, hour) VALUES (%(time_id)s, %(timestamp)s, %(year)s, %(month)s, %(day)s, %(hour)s)]
# [parameters: ({'time_id': 29, 'timestamp': '2024-09-02T20:38:00Z', 'year': 2024, 'month': 9, 'day': 2, 'hour': 20}, {'time_id': 30, 'timestamp': '2024-03-10T19:29:00Z', 'year': 2024, 'month': 3, 'day': 10, 'hour': 19}, {'time_id': 31, 'timestamp': '2024-09-03T05:00:00Z', 'year': 2024, 'month': 9, 'day': 3, 'hour': 5}, {'time_id': 32, 'timestamp': '2024-03-10T19:30:00Z', 'year': 2024, 'month': 3, 'day': 10, 'hour': 19}, {'time_id': 33, 'timestamp': '2024-03-10T19:31:00Z', 'year': 2024, 'month': 3, 'day': 10, 'hour': 19}, {'time_id': 34, 'timestamp': '2024-09-02T20:43:00Z', 'year': 2024, 'month': 9, 'day': 2, 'hour': 20}, {'time_id': 35, 'timestamp': '2024-09-03T15:50:23Z', 'year': 2024, 'month': 9, 'day': 3, 'hour': 15}, {'time_id': 36, 'timestamp': '2024-09-03T15:54:06Z', 'year': 2024, 'month': 9, 'day': 3, 'hour': 15}  ... displaying 10 of 27 total bound parameter sets ...  {'time_id': 54, 'timestamp': '2024-09-02T20:23:00Z', 'year': 2024, 'month': 9, 'day': 2, 'hour': 20}, {'time_id': 55, 'timestamp': '2024-09-02T22:44:06Z', 'year': 2024, 'month': 9, 'day': 2, 'hour': 22})]
# (Background on this error at: https://sqlalche.me/e/14/gkpj)
# [2024-09-02, 18:58:29 UTC] {logging_mixin.py:151} INFO - Spark session expired
# [2024-09-02, 18:58:29 UTC] {taskinstance.py:1935} ERROR - Task failed with exception
# Traceback (most recent call last):
#   File "/home/airflow/.local/lib/python3.8/site-packages/sqlalchemy/engine/base.py", line 1890, in _execute_context
#     self.dialect.do_executemany(
#   File "/home/airflow/.local/lib/python3.8/site-packages/sqlalchemy/dialects/postgresql/psycopg2.py", line 982, in do_executemany
#     context._psycopg2_fetched_rows = xtras.execute_values(
#   File "/home/airflow/.local/lib/python3.8/site-packages/psycopg2/extras.py", line 1299, in execute_values
#     cur.execute(b''.join(parts))
# psycopg2.errors.UniqueViolation: duplicate key value violates unique constraint "dimension_time_timestamp_key"
# DETAIL:  Key ("timestamp")=(2024-03-10 19:29:00+00) already exists.
# The above exception was the direct cause of the following exception:
# Traceback (most recent call last):
#   File "/home/airflow/.local/lib/python3.8/site-packages/airflow/operators/python.py", line 192, in execute
#     return_value = self.execute_callable()
#   File "/home/airflow/.local/lib/python3.8/site-packages/airflow/operators/python.py", line 209, in execute_callable
#     return self.python_callable(*self.op_args, **self.op_kwargs)
#   File "/opt/airflow/scripts/testSpark.py", line 532, in transform_store_postgreSQL
#     raise e
#   File "/opt/airflow/scripts/testSpark.py", line 529, in transform_store_postgreSQL
#     save_to_postgresql(df_location, df_parameter, df_time, df_facts, engine)
#   File "/opt/airflow/scripts/testSpark.py", line 488, in save_to_postgresql
#     raise e
#   File "/opt/airflow/scripts/testSpark.py", line 453, in save_to_postgresql
#     df.to_sql(table_name, conn, if_exists='append', index=False)
#   File "/home/airflow/.local/lib/python3.8/site-packages/pandas/core/generic.py", line 2878, in to_sql
#     return sql.to_sql(
#   File "/home/airflow/.local/lib/python3.8/site-packages/pandas/io/sql.py", line 769, in to_sql
#     return pandas_sql.to_sql(
#   File "/home/airflow/.local/lib/python3.8/site-packages/pandas/io/sql.py", line 1920, in to_sql
#     total_inserted = sql_engine.insert_records(
#   File "/home/airflow/.local/lib/python3.8/site-packages/pandas/io/sql.py", line 1470, in insert_records
#     raise err
#   File "/home/airflow/.local/lib/python3.8/site-packages/pandas/io/sql.py", line 1461, in insert_records
#     return table.insert(chunksize=chunksize, method=method)
#   File "/home/airflow/.local/lib/python3.8/site-packages/pandas/io/sql.py", line 1023, in insert
#     num_inserted = exec_insert(conn, keys, chunk_iter)
#   File "/home/airflow/.local/lib/python3.8/site-packages/pandas/io/sql.py", line 929, in _execute_insert
#     result = conn.execute(self.table.insert(), data)
#   File "/home/airflow/.local/lib/python3.8/site-packages/sqlalchemy/engine/base.py", line 1385, in execute
#     return meth(self, multiparams, params, _EMPTY_EXECUTION_OPTS)
#   File "/home/airflow/.local/lib/python3.8/site-packages/sqlalchemy/sql/elements.py", line 334, in _execute_on_connection
#     return connection._execute_clauseelement(
#   File "/home/airflow/.local/lib/python3.8/site-packages/sqlalchemy/engine/base.py", line 1577, in _execute_clauseelement
#     ret = self._execute_context(
#   File "/home/airflow/.local/lib/python3.8/site-packages/sqlalchemy/engine/base.py", line 1953, in _execute_context
#     self._handle_dbapi_exception(
#   File "/home/airflow/.local/lib/python3.8/site-packages/sqlalchemy/engine/base.py", line 2134, in _handle_dbapi_exception
#     util.raise_(
#   File "/home/airflow/.local/lib/python3.8/site-packages/sqlalchemy/util/compat.py", line 211, in raise_
#     raise exception
#   File "/home/airflow/.local/lib/python3.8/site-packages/sqlalchemy/engine/base.py", line 1890, in _execute_context
#     self.dialect.do_executemany(
#   File "/home/airflow/.local/lib/python3.8/site-packages/sqlalchemy/dialects/postgresql/psycopg2.py", line 982, in do_executemany
#     context._psycopg2_fetched_rows = xtras.execute_values(
#   File "/home/airflow/.local/lib/python3.8/site-packages/psycopg2/extras.py", line 1299, in execute_values
#     cur.execute(b''.join(parts))
# sqlalchemy.exc.IntegrityError: (psycopg2.errors.UniqueViolation) duplicate key value violates unique constraint "dimension_time_timestamp_key"
# DETAIL:  Key ("timestamp")=(2024-03-10 19:29:00+00) already exists.
# [SQL: INSERT INTO dimension_time (time_id, timestamp, year, month, day, hour) VALUES (%(time_id)s, %(timestamp)s, %(year)s, %(month)s, %(day)s, %(hour)s)]
# [parameters: ({'time_id': 29, 'timestamp': '2024-09-02T20:38:00Z', 'year': 2024, 'month': 9, 'day': 2, 'hour': 20}, {'time_id': 30, 'timestamp': '2024-03-10T19:29:00Z', 'year': 2024, 'month': 3, 'day': 10, 'hour': 19}, {'time_id': 31, 'timestamp': '2024-09-03T05:00:00Z', 'year': 2024, 'month': 9, 'day': 3, 'hour': 5}, {'time_id': 32, 'timestamp': '2024-03-10T19:30:00Z', 'year': 2024, 'month': 3, 'day': 10, 'hour': 19}, {'time_id': 33, 'timestamp': '2024-03-10T19:31:00Z', 'year': 2024, 'month': 3, 'day': 10, 'hour': 19}, {'time_id': 34, 'timestamp': '2024-09-02T20:43:00Z', 'year': 2024, 'month': 9, 'day': 2, 'hour': 20}, {'time_id': 35, 'timestamp': '2024-09-03T15:50:23Z', 'year': 2024, 'month': 9, 'day': 3, 'hour': 15}, {'time_id': 36, 'timestamp': '2024-09-03T15:54:06Z', 'year': 2024, 'month': 9, 'day': 3, 'hour': 15}  ... displaying 10 of 27 total bound parameter sets ...  {'time_id': 54, 'timestamp': '2024-09-02T20:23:00Z', 'year': 2024, 'month': 9, 'day': 2, 'hour': 20}, {'time_id': 55, 'timestamp': '2024-09-02T22:44:06Z', 'year': 2024, 'month': 9, 'day': 2, 'hour': 22})]
# (Background on this error at: https://sqlalche.me/e/14/gkpj)
# [2024-09-02, 18:58:30 UTC] {taskinstance.py:1398} INFO - Marking task as UP_FOR_RETRY. dag_id=air_quality_pipeline, task_id=transform_store_postgreSQL, execution_date=20240902T185604, start_date=20240902T185631, end_date=20240902T185830
# [2024-09-02, 18:58:31 UTC] {standard_task_runner.py:104} ERROR - Failed to execute job 7 for task transform_store_postgreSQL ((psycopg2.errors.UniqueViolation) duplicate key value violates unique constraint "dimension_time_timestamp_key"
# DETAIL:  Key ("timestamp")=(2024-03-10 19:29:00+00) already exists.
# [SQL: INSERT INTO dimension_time (time_id, timestamp, year, month, day, hour) VALUES (%(time_id)s, %(timestamp)s, %(year)s, %(month)s, %(day)s, %(hour)s)]
# [parameters: ({'time_id': 29, 'timestamp': '2024-09-02T20:38:00Z', 'year': 2024, 'month': 9, 'day': 2, 'hour': 20}, {'time_id': 30, 'timestamp': '2024-03-10T19:29:00Z', 'year': 2024, 'month': 3, 'day': 10, 'hour': 19}, {'time_id': 31, 'timestamp': '2024-09-03T05:00:00Z', 'year': 2024, 'month': 9, 'day': 3, 'hour': 5}, {'time_id': 32, 'timestamp': '2024-03-10T19:30:00Z', 'year': 2024, 'month': 3, 'day': 10, 'hour': 19}, {'time_id': 33, 'timestamp': '2024-03-10T19:31:00Z', 'year': 2024, 'month': 3, 'day': 10, 'hour': 19}, {'time_id': 34, 'timestamp': '2024-09-02T20:43:00Z', 'year': 2024, 'month': 9, 'day': 2, 'hour': 20}, {'time_id': 35, 'timestamp': '2024-09-03T15:50:23Z', 'year': 2024, 'month': 9, 'day': 3, 'hour': 15}, {'time_id': 36, 'timestamp': '2024-09-03T15:54:06Z', 'year': 2024, 'month': 9, 'day': 3, 'hour': 15}  ... displaying 10 of 27 total bound parameter sets ...  {'time_id': 54, 'timestamp': '2024-09-02T20:23:00Z', 'year': 2024, 'month': 9, 'day': 2, 'hour': 20}, {'time_id': 55, 'timestamp': '2024-09-02T22:44:06Z', 'year': 2024, 'month': 9, 'day': 2, 'hour': 22})]
# (Background on this error at: https://sqlalche.me/e/14/gkpj); 870)
# [2024-09-02, 18:58:31 UTC] {local_task_job_runner.py:228} INFO - Task exited with return code 1
# [2024-09-02, 18:58:31 UTC] {taskinstance.py:2776} INFO - 0 downstream tasks scheduled from follow-on schedule check
