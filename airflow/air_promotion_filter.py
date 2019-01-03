from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
import os


# Run script by using:
# spark-submit --packages mysql:mysql-connector-java:5.1.39,com.databricks:spark-avro_2.11:4.0.0 Promotion_Filter.py
# Main function

def main():


    spark = SparkSession.builder.master("local").appName("Initial Load").getOrCreate()
    last_update_column_name = 'last_update_date'


    hostname = "localhost"
    port = "3306"
    connection = "jdbc:mysql://"
    dbname = "foodmart"
    readdriver = "com.mysql.jdbc.Driver"
    username = "root"
    password = "mysql"

    destination_path = "file:///mnt/c/Users/Arthur/Documents/retail_ensoftek/buckets/"

    # STEP 2 - Load S3 Raw-Bucket to Cleansing S3 Raw-Bucket
    # *******************************************************************
    # *******************************************************************
    # *******************************************************************
    promotion_RF_df=spark.read.format("com.databricks.spark.avro").load(destination_path + "raw/promotion/")
    pro_df=promotion_RF_df.where(col("promotion_id") > 0).drop(last_update_column_name)

    sales_1997_RF_df=spark.read.format("com.databricks.spark.avro").load(destination_path + "raw/sales_fact_1997/")
    sales97_df=sales_1997_RF_df.where(col("promotion_id") > 0).drop(last_update_column_name)

    sales_1998_RF_df=spark.read.format("com.databricks.spark.avro").load(destination_path + "raw/sales_fact_1998/")
    sales98_df=sales_1998_RF_df.where(col("promotion_id") > 0).drop(last_update_column_name)

    # sales_1998_dec_RF_df=spark.read.format("com.databricks.spark.avro").load(
    #     "file://" + home + "/foodmart/case_study/raw/sales98dec/")
    # sales98_dec_df=sales_1998_dec_RF_df.where (col("promotion_id") > 0).drop('last_update_column')

    sales_DF=sales97_df.unionAll(sales98_df)

    final_DF=pro_df.join(sales_DF, pro_df.promotion_id == sales_DF.promotion_id).drop(sales_DF.promotion_id)

    # MAKE SURE YOU CREATE THE DIRECTORY FIRST, BECAUSE THIS IS APPENDING
    final_DF.write.mode("append").format("parquet").save(destination_path + "cleansed/")

    # *******************************************************************
    # *******************************************************************
    # *******************************************************************
    # STEP 2 - completed


if __name__ == "__main__":
        main()
