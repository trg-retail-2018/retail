# @Author: Arthur Shing
# @Date:   2019-01-02T14:02:10-08:00
# @Filename: pipeline.py
# @Last modified by:   Arthur Shing
# @Last modified time: 2019-01-02T14:29:16-08:00

from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta


import os
import sys

# Directory that holds all the scripts
script_home = "/mnt/c/Users/Arthur/Documents/retail_ensoftek/airflow/"

os.environ['SPARK_HOME'] = '/usr/lib/spark/'
sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'bin'))

dag = DAG(
    dag_id='foodmart',
    dagrun_timeout=timedelta(minutes=15)
)

start = DummyOperator(
	task_id='start',
	dag=dag
)

initial_load = BashOperator(
    task_id='initial_load',
    bash_command='spark-submit --packages {{ params.pkgs }} {{ params.file }}',
    params={'pkgs': 'mysql:mysql-connector-java:5.1.39,com.databricks:spark-avro_2.11:4.0.0', 'file': script_home + 'air_initial_load.py'},
    dag=dag
)

incremental_load = BashOperator(
    task_id='incremental_load',
    bash_command='spark-submit --packages {{ params.pkgs }} {{ params.file }}',
    params={'pkgs': 'mysql:mysql-connector-java:5.1.39,com.databricks:spark-avro_2.11:4.0.0', 'file': script_home + 'air_incremental_load.py'},
    dag=dag
)

promotion_filter = BashOperator(
    task_id='promotion_filter',
    bash_command='spark-submit --packages {{ params.pkgs }} {{ params.file }}',
    params={'pkgs': 'mysql:mysql-connector-java:5.1.39,com.databricks:spark-avro_2.11:4.0.0', 'file': script_home + 'air_promotion_filter.py'},
    dag=dag
)

aggregate = BashOperator(
    task_id='aggregate',
    bash_command='spark-submit --packages {{ params.pkgs }} {{ params.file }}',
    params={'pkgs': 'mysql:mysql-connector-java:5.1.39,com.databricks:spark-avro_2.11:4.0.0', 'file': script_home + 'air_aggregate.py'},
    dag=dag
)

end = DummyOperator(
        task_id='end',
        dag=dag
)

start >> initial_load >> promotion_filter >> aggregate >> end
