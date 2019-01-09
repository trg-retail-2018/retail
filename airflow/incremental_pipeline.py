# @Author: Arthur Shing
# @Date:   2019-01-02T14:02:10-08:00
# @Filename: pipeline.py
# @Last modified by:   Arthur Shing
# @Last modified time: 2019-01-09T11:17:09-08:00

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import ShortCircuitOperator
from datetime import datetime, timedelta
from pyspark.sql import SparkSession


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
    spark = SparkSession.builder.master("local").appName("Initial Load").getOrCreate()
    # Set parameters for reading
    hostname = "localhost"
    port = "3306"
    connection = "jdbc:mysql://"
    dbname = "foodmart"
    readdriver = "com.mysql.jdbc.Driver"
    username = "root"
    password = "mysql"
    destination_path = "s3a://ashiraw/foodmart/"

    tablenames = ["promotion", "sales_fact_1997", "sales_fact_1998"]

    sum_new_data = 0
    for table in tablenames:
        # Try loading the last updated dates
        try:
            last_date_table = spark.read.csv(destination_path + "last_updated_dates/" + table)
        except e:
            print("Could not find path to last updated dates files")
        # Convert last updated dates from string to datetime
        last_date = datetime.strptime(str(last_date_table.first()._c0), '%Y-%m-%d %H:%M:%S')
        # Create dataframe. Mysqlconnector package is required for the driver
        df = spark.read.format("jdbc").options(
            url= connection + hostname + ':' + port + '/' + dbname,
            driver = readdriver,
            dbtable = table,
            user=username,
            password=password).load()
        new_data = df.where(df.last_update_date > last_date)
        sum_new_data += new_data.count()
    if(sum_new_data > 0):
        return True
    else:
        return False

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

start >> check_for_data >> incremental_load >> promotion_filter >> aggregate >> load_snowflake >> query_snowflake >> end
