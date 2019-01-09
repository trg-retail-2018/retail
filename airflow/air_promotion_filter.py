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

    source_path = "s3a://ashiraw/foodmart/"
    destination_path = "s3a://ashicleansed/foodmart/"

    # STEP 2 - Load S3 Raw-Bucket to Cleansing S3 Raw-Bucket
    # *******************************************************************
    # *******************************************************************
    # *******************************************************************
    promotion_df=spark.read.format("com.databricks.spark.avro").load(source_path + "raw/promotion/").where(col("promotion_id") > 0).drop(last_update_column_name)

    sales_1997_df=spark.read.format("com.databricks.spark.avro").load(source_path + "raw/sales_fact_1997/").where(col("promotion_id") > 0).drop(last_update_column_name)

    sales_1998_df=spark.read.format("com.databricks.spark.avro").load(source_path + "raw/sales_fact_1998/").where(col("promotion_id") > 0).drop(last_update_column_name)

    sales_DF=sales_1997_df.unionAll(sales_1998_df)

    final_DF=promotion_df.join(sales_DF, "promotion_id").drop(sales_DF.promotion_id)

    final_DF.coalesce(2).write.mode("overwrite").format("parquet").save(destination_path)

    # *******************************************************************
    # *******************************************************************
    # *******************************************************************
    # STEP 2 - completed


if __name__ == "__main__":
        main()
