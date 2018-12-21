from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
import datetime
#import org.apache.spark.sql.SaveMode

# Run script by using:
# spark-submit2 --packages mysql:mysql-connector-java:5.1.39,com.databricks:spark-avro_2.11:4.0.0 initial_load.py

#Main function

def main():

	# Set up spark context 

	spark = SparkSession.builder.master("local").appName("Initial Load").getOrCreate()

	pro_RF_df = spark.read.format("com.databricks.spark.avro").load("/home/cloudera/Shwetha/case_study/raw/promotion/")
	pro_df = pro_RF_df.where(col("promotion_id") > 0).drop("lst_upd_date")
	sales97_RF_df = spark.read.format("com.databricks.spark.avro").load("/home/cloudera/Shwetha/case_study/raw/sales97/")
	sales97_df = sales97_RF_df.where(col("promotion_id") > 0).drop("lst_upd_date")
	sales98_RF_df = spark.read.format("com.databricks.spark.avro").load("/home/cloudera/Shwetha/case_study/raw/sales98/")
	sales98_df = sales98_RF_df.where(col("promotion_id") > 0).drop("lst_upd_date")
	

	print pro_df.show()
	print sales97_df.show()
	print sales98_df.show()	
	
	sales_DF = sales97_df.unionAll(sales98_df)
	final_DF = pro_df.join(sales_DF, pro_df.promotion_id == sales_DF.promotion_id).drop(sales_DF.promotion_id)
	#final_DF.write.mode(SaveMode.Append).format("parquet").save("/home/cloudera/Shwetha/case_study/parquet")
	final_DF.write.format("parquet").save("file:///home/cloudera/Shwetha/case_study/parquet")

	print sales_DF.show()
	print final_DF.show()

# Runs the script
if __name__ == "__main__":
	main()
