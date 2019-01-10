# @Author: Arthur Shing
# @Date:   2019-01-02T14:02:10-08:00
# @Filename: pipeline.py
# @Last modified by:   Arthur Shing
# @Last modified time: 2019-01-09T14:45:33-08:00

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import ShortCircuitOperator
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
import boto3
import botocore



import os
import sys

"""
COPY THIS FILE INTO YOUR DAGS FOLDER (MKDIR IF NOT EXIST), DEFAULT IS ~/airflow/dags (CHECK ~/airflow/airflow.cfg)
"""


# Directory that holds all the scripts
script_home = "/mnt/c/Users/Arthur/Documents/retail_ensoftek/airflow/"

# Make sure spark-submit exists, replace '/usr/lib/spark/' with wherever your spark home is
os.environ['SPARK_HOME'] = '/usr/lib/spark/'
sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'bin'))

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 4),
    'email': ['shinga@oregonstate.edu'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=20),
}

dag = DAG(
    dag_id='foodmart_incremental',
    catchup=False,
    default_args=default_args,
    dagrun_timeout=timedelta(minutes=15),
    schedule_interval='@hourly',
)

start = DummyOperator(
	task_id='start',
	dag=dag
)


def check_new_data():
    s3 = boto3.resource('s3')
    try:
        s3.Object('ashiraw', 'foodmart/yes_new_data').load()
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            return False
        else:
            raise
    else:
        s3.Object('ashiraw', 'foodmart/yes_new_data').delete()
        return True

        # Get the new rows (where the column last_update_date is greater (Newer) than the previously logged last_update_date)


check_for_data = ShortCircuitOperator(
    task_id='check_for_data',
    python_callable=check_new_data,
    dag=dag
)


incremental_load = BashOperator(
    task_id='incremental_load',
    bash_command='spark-submit --jars {{ params.jars }} --packages {{ params.pkgs }} {{ params.file }}',
    params={'jars': '/usr/local/bin/aws-java-sdk-1.7.4.jar,/usr/local/bin/hadoop-aws-2.7.3.jar', 'pkgs': 'mysql:mysql-connector-java:5.1.39,com.databricks:spark-avro_2.11:4.0.0', 'file': script_home + 'air_incremental_load.py'},
    dag=dag
)

promotion_filter = BashOperator(
    task_id='promotion_filter',
    bash_command='spark-submit --jars {{ params.jars }} --packages {{ params.pkgs }} {{ params.file }}',
    params={'jars': '/usr/local/bin/aws-java-sdk-1.7.4.jar,/usr/local/bin/hadoop-aws-2.7.3.jar', 'pkgs': 'mysql:mysql-connector-java:5.1.39,com.databricks:spark-avro_2.11:4.0.0', 'file': script_home + 'air_promotion_filter.py'},
    dag=dag
)

aggregate = BashOperator(
    task_id='aggregate',
    bash_command='spark-submit --jars {{ params.jars }} --packages {{ params.pkgs }} {{ params.file }}',
    params={'jars': '/usr/local/bin/aws-java-sdk-1.7.4.jar,/usr/local/bin/hadoop-aws-2.7.3.jar', 'pkgs': 'mysql:mysql-connector-java:5.1.39,com.databricks:spark-avro_2.11:4.0.0', 'file': script_home + 'air_aggregate.py'},
    dag=dag
)

load_snowflake = BashOperator(
    task_id='load_snowflake',
    bash_command='spark-submit --jars {{ params.jars }} --packages {{ params.pkgs }} {{ params.file }}',
    params={'jars': '/usr/local/bin/aws-java-sdk-1.7.4.jar,/usr/local/bin/hadoop-aws-2.7.3.jar', 'pkgs': 'mysql:mysql-connector-java:5.1.39,com.databricks:spark-avro_2.11:4.0.0', 'file': script_home + 'air_snowflake_load.py'},
    dag=dag
)

query_snowflake = BashOperator(
    task_id='query_snowflake',
    bash_command='spark-submit --jars {{ params.jars }} --packages {{ params.pkgs }} {{ params.file }}',
    params={'jars': '/usr/local/bin/aws-java-sdk-1.7.4.jar,/usr/local/bin/hadoop-aws-2.7.3.jar', 'pkgs': 'mysql:mysql-connector-java:5.1.39,com.databricks:spark-avro_2.11:4.0.0', 'file': script_home + 'air_snowflake_query.py'},
    dag=dag
)

end = DummyOperator(
        task_id='end',
        dag=dag
)

start >> incremental_load >> check_for_data >> promotion_filter >> aggregate >> load_snowflake >> query_snowflake >> end
