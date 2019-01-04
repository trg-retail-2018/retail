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

	destination_path = "s3a://ashiraw/foodmart/"

	parquet_df = spark.read.format("parquet").load(destination_path + "cleansed")

	timeByDay_df = spark.read.format("com.databricks.spark.avro").load(destination_path + "raw/time_by_day/")

	store_df = spark.read.format("com.databricks.spark.avro").load(destination_path + "raw/store/")

	prq_time_store_df = parquet_df.join(timeByDay_df, "time_id").join(store_df, "store_id")

	# Cut out all the junk data
	prq_time_store_df = prq_time_store_df.select("region_id", "promotion_id", "cost", "store_sales", "the_day", "the_month", "the_year", store_df.region_id)

	#register as temp table so that you will be able to query the table
	prq_time_store_df.registerTempTable("myTable")
	weekday_DF = spark.sql("SELECT region_id,promotion_id,the_month,the_year,sum(store_sales) as weekday_sales FROM myTable WHERE the_day NOT IN ('Saturday', 'Sunday') GROUP BY region_id,promotion_id,the_month,the_year")
	weekend_DF = spark.sql("SELECT region_id,promotion_id,the_month,the_year,sum(store_sales) as weekend_sales FROM myTable WHERE the_day IN ('Saturday', 'Sunday') GROUP BY region_id,promotion_id,the_month,the_year ").drop("region_id")


	csv_df = weekday_DF.join(weekend_DF,"promotion_id")

	csv_df.write.mode("overwrite").format("csv").save(destination_path + "aggregate")


# Runs the script
if __name__ == "__main__":
	main()
