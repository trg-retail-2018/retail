import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators import DummyOperator
from datetime import date, datetime, timedelta
import os

# To run entire dag:
# shell> airflow resetdb
# shell> airflow webserver -p 9990
# shell> airflow scheduler
# wait for the dag to be triggered by scheduler! and hopefully it wont fail! :)
# To test one task like initial_load for example run this with today's date in YYYY-MM-DD format:
# shell> airflow webserver -p 9990
# shell> airflow test retail_dag initial_load <today's_date>
# To get these airflow libraries for pycharm run this in windows shell
# pip install apache-airflow --no-deps

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': '2019-01-01'
}

dag = DAG(
    dag_id='pipeline_dag',
    default_args=default_args,
    description='our casestudy',
    schedule_interval=timedelta(days=1)
)

t1 = BashOperator(
    task_id='initial_load',
    bash_command="spark-submit --packages mysql:mysql-connector-java:5.1.39,org.apache.spark:spark-avro_2.11:2.4.0 /mnt/c/dev/casestudy/airflow/initial_load.py ",
    dag=dag
)

t2 = BashOperator(
    task_id='incremental_load',
    bash_command="spark-submit --packages mysql:mysql-connector-java:5.1.39,org.apache.spark:spark-avro_2.11:2.4.0 /mnt/c/dev/casestudy/airflow/incremental_load.py ",
    dag=dag
)

#t3 = BashOperator(
#    task_id='promotion_filter',
#    bash_command="spark-submit --packages mysql:mysql-connector-java:5.1.39,org.apache.spark:spark-avro_2.11:2.4.0 /mnt/c/dev/casestudy/airflow/promotion_filter.py ",
#    dag=dag
#)

#t4 = BashOperator(
#   task_id='aggregate_load',
#   bash_command="spark-submit --packages mysql:mysql-connector-java:5.1.39,org.apache.spark:spark-avro_2.11:2.4.0 /mnt/c/dev/casestudy/airflow/aggregate_load.py ",
#   dag=dag
#)


#t1 >> t3 >> t4
t1 >> t2
#t3.set_upstream(t1)