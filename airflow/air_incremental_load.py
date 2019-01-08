from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession, SQLContext
import datetime
import time
import os


# Run script by using:
# spark-submit --jars /usr/local/bin/aws-java-sdk-1.7.4.jar,/usr/local/bin/hadoop-aws-2.7.3.jar --packages mysql:mysql-connector-java:5.1.39,com.databricks:spark-avro_2.11:4.0.0 air_incremental_load.py

#Main function
def main():


	# Create the spark session, this replaces the sparkcontext and sqlcontext we had earlier
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

	for table in tablenames:
		# Try loading the last updated dates
		try:
			last_date_table = spark.read.csv(destination_path + "last_updated_dates/" + table)
		except e:
			print("Could not find path to last updated dates files")

		# Convert last updated dates from string to datetime
		last_date = datetime.datetime.strptime(str(last_date_table.first()._c0), '%Y-%m-%d %H:%M:%S')

		# Create dataframe. Mysqlconnector package is required for the driver
		df = spark.read.format("jdbc").options(
		    url= connection + hostname + ':' + port + '/' + dbname,
		    driver = readdriver,
		    dbtable = table,
		    user=username,
		    password=password).load()

		# Get the new rows (where the column last_update_date is greater (Newer) than the previously logged last_update_date)
		new_data = df.where(df.last_update_date > last_date)

		new_data.write.mode("append").format("com.databricks.spark.avro").save(destination_path + "raw/" + table)
		dfmax = df.agg({"last_update_date": "max"})
		dfmax.write.mode("overwrite").option("timestampFormat", "yyyy-MM-dd HH:mm:ss").format("csv").save(destination_path + "last_updated_dates/" + table)



# Runs the script
if __name__ == "__main__":
	main()
