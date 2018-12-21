from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession, SQLContext
import datetime
import time
import os


# Run script by using:
# spark-submit --packages mysql:mysql-connector-java:5.1.39,com.databricks:spark-avro_2.11:4.0.0 initial_load.py

#Main function
def main():


	# Create the spark session, this replaces the sparkcontext and sqlcontext we had earlier
    spark = SparkSession.builder \
         .master("local") \
         .appName("Initial Load") \
         .getOrCreate()

	# Set parameters for reading
	hostname = "nn01.itversity.com"
	port = "3306"
	connection = "jdbc:mysql://"
	dbname = "retail_export"
	readdriver = "com.mysql.jdbc.Driver"
	username = "retail_dba"
	password = "itversity"

	# Read the last updated dates from a file
	try:
		home = os.path.expandvars("$HOME")
		plast = spark.read.csv("file://" + home + "/foodmart/case_study/last_updated_dates/promotion")
		s97last = spark.read.csv("file://" + home + "/foodmart/case_study/last_updated_dates/sales97")
		s98last = spark.read.csv("file://" + home + "/foodmart/case_study/last_updated_dates/sales98")
	except e:
		print("Could not find path to last updated dates files")

	# Convert last updated dates from string to datetime
	plastdate = datetime.datetime.strptime(str(plast.first()._c0), '%Y-%m-%d %H:%M:%S')
	s97lastdate = datetime.datetime.strptime(str(s97last.first()._c0), '%Y-%m-%d %H:%M:%S')
	s98lastdate = datetime.datetime.strptime(str(s98last.first()._c0), '%Y-%m-%d %H:%M:%S')

	# Create promotion dataframe. Mysqlconnector package is required for the driver
	# Change url to jdbc:mysql://${HOSTNAME}:3306/${DATABASE_NAME}
	# Change user, dbtable and password accordingly
	promotion_df = spark.read.format("jdbc").options(
	    url= connection + hostname + ':' + port + '/' + dbname,
	    driver = readdriver,
	    dbtable = "as_promotion",
	    user=username,
	    password=password).load()

	sales_1997_df = spark.read.format("jdbc").options(
            url=connection + hostname + ':' + port + '/' + dbname,
            driver = readdriver,
            dbtable = "as_sales_1997",
            user=username,
            password=password).load()

	sales_1998_df = spark.read.format("jdbc").options(
            url=connection + hostname + ':' + port + '/' + dbname,
            driver = readdriver,
            dbtable = "as_sales_1998",
            user=username,
            password=password).load()


	# Get the new rows (where the column last_update_date is greater (Newer) than the previously logged last_update_date)
	newp = promotion_df.where(promotion_df.last_update_date > plastdate).select("last_update_date")
	news97 = sales_1997_df.where(sales_1997_df.last_update_date > s97lastdate).select("last_update_date")
	news98 = sales_1998_df.where(sales_1998_df.last_update_date > s98lastdate).select("last_update_date")

	# Debugging purposes:
	# Just a print statement to see if the dataframe transferred sucessfully
	print promotion_df.show()
	print sales_1997_df.show()
	print sales_1998_df.show()
	# Just a print statement to see if there are any new rows
	print newp.show(10)
	print news97.show(10)
	print news98.show(10)


	#
	newp.write.mode("append").format("com.databricks.spark.avro").save("file:///home/arthurshing/foodmart/case_study/raw/promotion")
	news97.write.mode("append").format("com.databricks.spark.avro").save("file:///home/arthurshing/foodmart/case_study/raw/sales97")
	news98.write.mode("append").format("com.databricks.spark.avro").save("file:///home/arthurshing/foodmart/case_study/raw/sales98")

	# Get the new last updated date
	pmax = promotion_df.agg({"last_update_date": "max"})
	s97max = sales_1997_df.agg({"last_update_date": "max"})
	s98max = sales_1998_df.agg({"last_update_date": "max"})

	# Write new last updated date to file
	pmax.write.option("timestampFormat", "yyyy-MM-dd HH:mm:ss", mode='overwrite').format("csv").save("file:///home/arthurshing/foodmart/case_study/last_updated_dates/promotion")
	s97max.write.option("timestampFormat", "yyyy-MM-dd HH:mm:ss", mode='overwrite').format("csv").save("file:///home/arthurshing/foodmart/case_study/last_updated_dates/sales97")
	s98max.write.option("timestampFormat", "yyyy-MM-dd HH:mm:ss", mode='overwrite').format("csv").save("file:///home/arthurshing/foodmart/case_study/last_updated_dates/sales98")



# Runs the script
if __name__ == "__main__":
	main()
