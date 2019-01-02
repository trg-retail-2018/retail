from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession, SQLContext
import datetime
import shutil

# Run script by using:
# spark-submit --packages mysql:mysql-connector-java:5.1.39,com.databricks:spark-avro_2.11:4.0.0 initial_load.py

#Main function
def main():

	spark = SparkSession.builder.master("local").appName("Initial Load").getOrCreate()


	# Set parameters for reading
	hostname = "localhost"
	port = "3306"
	connection = "jdbc:mysql://"
	dbname = "foodmart"
	readdriver = "com.mysql.jdbc.Driver"
	username = "root"
	password = "mysql"

	foldpath = "/mnt/c/Users/Arthur/Documents/retail_ensoftek/buckets/"
	destination_path = "file://" + foldpath

	shutil.rmtree(destination_path)

	# Create promotion dataframe. Mysqlconnector package is required for the driver
	# Change url to jdbc:mysql://${HOSTNAME}:3306/${DATABASE_NAME}
	# Change user, dbtable and password accordingly
	promotion_df = spark.read.format("jdbc").options(
	    url= connection + hostname + ':' + port + '/' + dbname,
	    driver = readdriver,
	    dbtable = "promotion",
	    user=username,
	    password=password).load()

	sales_1997_df = spark.read.format("jdbc").options(
            url=connection + hostname + ':' + port + '/' + dbname,
            driver = readdriver,
            dbtable = "sales_fact_1997",
            user=username,
            password=password).load()

	sales_1998_df = spark.read.format("jdbc").options(
            url=connection + hostname + ':' + port + '/' + dbname,
            driver = readdriver,
            dbtable = "sales_fact_1998",
            user=username,
            password=password).load()

	# Just a print statement to see if the dataframe transferred sucessfully
	print promotion_df.show()
	print sales_1997_df.show()
	print sales_1998_df.show()

	promotion_df.write.format("com.databricks.spark.avro").save(destination_path + "raw/promotion")
	sales_1997_df.write.format("com.databricks.spark.avro").save(destination_path + "raw/sales97")
	sales_1998_df.write.format("com.databricks.spark.avro").save(destination_path + "raw/sales98")

	pmax = promotion_df.agg({"last_update_date": "max"})
	s97max = sales_1997_df.agg({"last_update_date": "max"})
	s98max = sales_1998_df.agg({"last_update_date": "max"})

	pmax.write.option("timestampFormat", "yyyy-MM-dd HH:mm:ss").format("csv").save(destination_path + "last_updated_dates/promotion")
	s97max.write.option("timestampFormat", "yyyy-MM-dd HH:mm:ss").format("csv").save(destination_path + "last_updated_dates/sales97")
	s98max.write.option("timestampFormat", "yyyy-MM-dd HH:mm:ss").format("csv").save(destination_path + "last_updated_dates/sales98")



# Runs the script
if __name__ == "__main__":
	main()
