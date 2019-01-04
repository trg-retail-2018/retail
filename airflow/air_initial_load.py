from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession, SQLContext
import datetime
import shutil
import boto3
from os.path import expanduser


# Run script by using:
# spark-submit --jars /usr/local/bin/aws-java-sdk-1.7.4.jar,/usr/local/bin/hadoop-aws-2.7.3.jar --packages mysql:mysql-connector-java:5.1.39,com.databricks:spark-avro_2.11:4.0.0 air_initial_load.py

# I don't think this does anything
# def loadCredentials(filepath):
# 	f = open(filepath, "r")
# 	f.readline()
# 	ak = f.readline()[18:].strip()
# 	sk = f.readline()[22:].strip()
# 	return (ak, sk)

#Main function
def main():

	spark = SparkSession.builder.master("local").appName("Initial Load").getOrCreate()

	# Not sure if we need this, but this sets s3 credentials for spark, I think.
	# I don't think this does anything.
	# home = expanduser("~") # Gets home directory (/home/shinga/)
	# ACCESS_KEY, SECRET_KEY = loadCredentials(home + "/.aws/credentials")
	# spark._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", ACCESS_KEY)
	# spark._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", SECRET_KEY)

	# Set parameters for reading
	hostname = "localhost"
	port = "3306"
	connection = "jdbc:mysql://"
	dbname = "foodmart"
	readdriver = "com.mysql.jdbc.Driver"
	username = "root"
	password = "mysql"

	# Set the target path - "s3a://[bucketname]/[folder]/"
	destination_path = "s3a://ashiraw/foodmart/"

	# destination_path = "/mnt/c/Users/Arthur/Documents/retail_ensoftek/buckets/"


	# Create promotion dataframe. Mysqlconnector package is required for the driver
	# Change url to jdbc:mysql://${HOSTNAME}:3306/${DATABASE_NAME}
	# Change user, dbtable and password accordingly

	# Instead of loading them uglily, here is a pretty version in a for-loop
	# This version writes to s3a://[bucket]/foodmart/raw/[table-name]/
	# Since we named some folders as "sales97" before and not "sales_fact_1997", make sure you change them in the other scripts
	# We are loading time_by_day and store here as well, so we don't have to load them later in aggregate. Haven't fixed that yet.

	# This puts the table names into a list
	tablenames = ["promotion", "sales_fact_1997", "sales_fact_1998", "time_by_day", "store"]

	for table in tablenames:
		df = spark.read.format("jdbc").options(
		    url= connection + hostname + ':' + port + '/' + dbname,
		    driver = readdriver,
		    dbtable = table,
		    user=username,
		    password=password).load()

		df.coalesce(1).write.mode("overwrite").format("com.databricks.spark.avro").save(destination_path + "raw/" + table)

		if table in ["promotion", "sales_fact_1997", "sales_fact_1998"]:
			dfmax = df.agg({"last_update_date": "max"})
			dfmax.coalesce(1).write.mode("overwrite").option("timestampFormat", "yyyy-MM-dd HH:mm:ss").format("csv").save(destination_path + "last_updated_dates/" + table)



# Runs the script
if __name__ == "__main__":
	main()
