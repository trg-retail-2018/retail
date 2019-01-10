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

	# Set up paths
	source_path_cleansed = "s3a://ashicleansed/foodmart/"
	source_path_raw = "s3a://ashiraw/foodmart/"
	destination_path = "s3a://ashicurated/foodmart/"

	# Set up spark context
	spark = SparkSession.builder.master("local").appName("Initial Load").getOrCreate()

	# Load files
	parquet_df = spark.read.format("parquet").load(source_path_cleansed)
	timeByDay_df = spark.read.format("com.databricks.spark.avro").load(source_path_raw + "raw/time_by_day/")
	store_df = spark.read.format("com.databricks.spark.avro").load(source_path_raw + "raw/store/")
	prq_time_store_df = parquet_df.join(timeByDay_df, "time_id").join(store_df, "store_id")

	# Cut out all the junk data
	prq_time_store_df = prq_time_store_df.select("region_id", "promotion_id", "cost", "store_sales", "the_day", "the_month", "the_year", store_df.region_id)

	# Here is what Michael did:
	# Filter the df for saturday, and union it with a df filtered for sunday => weekend dataframe
	# Filter the df for everything that isn't saturday or sunday => weekday dataframe
	weekend_DF = prq_time_store_df.filter("the_day == 'Saturday'").union(prq_time_store_df.filter("the_day == 'Sunday'"))
	weekday_DF = prq_time_store_df.filter("the_day != 'Saturday'").filter("the_day != 'Sunday'")

	# Cast the 'store_sales' column into a Double type (right now it is a String and cannot be aggregated) and rename it 'sales'
	weekend_DF = weekend_DF.withColumn("sales", weekend_DF["store_sales"].cast(DoubleType())).drop("store_sales")
	weekday_DF = weekday_DF.withColumn("sales", weekday_DF["store_sales"].cast(DoubleType())).drop("store_sales")

	# Aggregate: This is the same as
	# "SELECT region_id,promotion_id,the_month,the_year,sum(store_sales) FROM weekend_df GROUP BY region_id,promotion_id,the_month,the_year,cost"
	weekend_DF = weekend_DF.groupby("region_id", "promotion_id", "the_year", "the_month", "cost").agg(sum("sales").alias("weekend_sales"))
	weekday_DF = weekday_DF.groupby("region_id", "promotion_id", "the_year", "the_month", "cost").agg(sum("sales").alias("weekday_sales"))

	# Michael did something convoluted here, we're just going to equijoin
	# Join on multiple columns
	csv_df = weekday_DF.join(weekend_DF,["promotion_id", "region_id", "the_year", "the_month", "cost"], "fullouter")
	csv_df.na.fill(0, Seq("weekend_sales"))
	csv_df.na.fill(0, Seq("weekday_sales"))


	# Coalesce(6) works for me
	csv_df.coalesce(6).write.mode("overwrite").format("csv").save(destination_path + "aggregate")




# Runs the script
if __name__ == "__main__":
	main()
