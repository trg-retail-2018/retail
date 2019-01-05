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

	# Set parameters for reading
	hostname = "localhost"
	port = "3306"
	connection = "jdbc:mysql://"
	dbname = "foodmart"
	readdriver = "com.mysql.jdbc.Driver"
	username = "root"
	password = "mysql"

	destination_path = "file:///mnt/c/Users/Arthur/Documents/retail_ensoftek/buckets/"

	parquet_df = spark.read.format("parquet").load(destination_path + "cleansed")
	print parquet_df.show()

	timeByDay_df = spark.read.format("jdbc").options(
	    url= connection + hostname + ':' + port + '/' + dbname,
	    driver = readdriver,
	    dbtable = "time_by_day",
	    user=username,
	    password=password).load()

	store_df = spark.read.format("jdbc").options(
	    url= connection + hostname + ':' + port + '/' + dbname,
	    driver = readdriver,
	    dbtable = "store",
	    user=username,
	    password=password).load()

	# print timeByDay_df.show()
	# print store_df.show()

	parquet_time_df = parquet_df.join(timeByDay_df, parquet_df.time_id == timeByDay_df.time_id)
	prq_time_store_df = parquet_time_df.join(store_df, parquet_time_df.store_id == store_df.store_id)

	# print parquet_time_df.show()
	# print prq_time_store_df.show()

	#register as temp table so that you will be able to query the table
	prq_time_store_df.registerTempTable("myTable")
	weekday_DF = spark.sql("SELECT region_id,promotion_id,the_month,the_year,sum(store_sales) as weekday_sales FROM myTable WHERE the_day NOT IN ('Saturday', 'Sunday') GROUP BY region_id,promotion_id,the_month,the_year")
	weekend_DF = spark.sql("SELECT region_id,promotion_id,the_month,the_year,sum(store_sales) as weekend_sales FROM myTable WHERE the_day IN ('Saturday', 'Sunday') GROUP BY region_id,promotion_id,the_month,the_year ")
	# print weekday_DF.show()
	# print weekend_DF.show()

	csv_df = weekday_DF.join(weekend_DF, weekday_DF.promotion_id == weekend_DF.promotion_id).select(weekday_DF.promotion_id,weekday_DF.region_id,weekday_DF.the_month,weekday_DF.the_year,weekday_DF.weekday_sales,weekend_DF.weekend_sales)
	#weekday_DF.repartition(1).write.format("csv").save("file:///home/cloudera/Shwetha/case_study/csvweekday")
	#weekend_DF.repartition(1).write.format("csv").save("file:///home/cloudera/Shwetha/case_study/csvweekend")
	csv_df.repartition(2).write.format("csv").save(destination_path + "aggregate")
	print csv_df.show()



# Runs the script
if __name__ == "__main__":
	main()
