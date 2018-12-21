from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *

# Run script by using:
# spark-submit --packages mysql:mysql-connector-java:5.1.39,com.databricks:spark-avro_2.11:4.0.0 initial_load.py
# Main function

def main():
    # Set up spark context
    sc=SparkContext ('local[2]',"NetworkWordCount")

    # Set up sql context
    sqlContext=SQLContext(sc)

    

# STEP 2 - Load S3 Raw-Bucket to Cleansing S3 Raw-Bucket 
    # *******************************************************************
    # *******************************************************************
    # *******************************************************************
    promotion_RF_df=sqlContext.read.format("com.databricks.spark.avro").load(
        "/home/cloudera/foodmart/case_study/Avro/promotion/")
    pro_df=promotion_RF_df.where(col("promotion_id") > 0).drop('last_update_column')

    sales_1997_RF_df=sqlContext.read.format("com.databricks.spark.avro").load(
        "/home/cloudera/foodmart/case_study/Avro/sales97/")
    sales97_df=sales_1997_RF_df.where(col("promotion_id") > 0).drop('last_update_column')

    sales_1998_RF_df=sqlContext.read.format("com.databricks.spark.avro").load(
        "/home/cloudera/foodmart/case_study/Avro/sales98/")
    sales98_df=sales_1998_RF_df.where(col("promotion_id") > 0).drop('last_update_column')

    sales_1998_dec_RF_df=sqlContext.read.format("com.databricks.spark.avro").load(
        "/home/cloudera/foodmart/case_study/Avro/sales98dec/")
    sales98_dec_df=sales_1998_dec_RF_df.where (col("promotion_id") > 0).drop('last_update_column')

    sales_DF=sales97_df.unionAll(sales98_df)
    
    final_DF=pro_df.join(sales_DF, pro_df.promotion_id == sales_DF.promotion_id).drop(sales_DF.promotion_id)
    
    final_DF.write.mode("append").format("parquet").save("home/cloudera/foodmart/case_study/Parquet/promotion")
    
    print promotion_RF_df.show()
    print sales_DF.show()
    print final_DF.show()
    #print sales97_df.show()
    #print sales98_df.show()

	#print sales_DF.show()

	#print final_DF.show()
    
    # *******************************************************************
    # *******************************************************************
    # *******************************************************************
    # STEP 2 - completed
    
    
if __name__ == "__main__":
        main()
    
