from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession, SQLContext


# Run script by using:
# spark-submit --packages mysql:mysql-connector-java:5.1.39,com.databricks:spark-avro_2.11:4.0.0 initial_load.py

#Main function
def main():

	# Set up spark context 
        sc = SparkContext("local[2]", "NetworkWordCount")
	# Set up sql context
	sqlContext = SQLContext(sc)

	# Create promotion dataframe. Mysqlconnector package is required for the driver
	# Change url to jdbc:mysql://${HOSTNAME}:3306/${DATABASE_NAME}
	# Change user, dbtable and password accordingly 
	promotion_df = sqlContext.read.format("jdbc").options(
	    url="jdbc:mysql://nn01.itversity.com:3306/retail_export",
	    driver = "com.mysql.jdbc.Driver",
	    dbtable = "as_promotion",
	    user="retail_dba",
	    password="itversity").load()


	sales_1997_df = sqlContext.read.format("jdbc").options(
            url="jdbc:mysql://nn01.itversity.com:3306/retail_export",
            driver = "com.mysql.jdbc.Driver",
            dbtable = "as_sales_1997",
            user="retail_dba",
            password="itversity").load()



	sales_1998_df = sqlContext.read.format("jdbc").options(
            url="jdbc:mysql://nn01.itversity.com:3306/retail_export",
            driver = "com.mysql.jdbc.Driver",
            dbtable = "as_sales_1998",
            user="retail_dba",
            password="itversity").load()	
	# Just a print statement to see if the dataframe transferred sucessfully
	print promotion_df.show()
	print sales_1997_df.show()
	print sales_1998_df.show()

	promotion_df.write.format("com.databricks.spark.avro").save("file:///home/arthurshing/foodmart/case_study/raw/promotion")
	sales_1997_df.write.format("com.databricks.spark.avro").save("file:///home/arthurshing/foodmart/case_study/raw/sales97")
	sales_1998_df.write.format("com.databricks.spark.avro").save("file:///home/arthurshing/foodmart/case_study/raw/sales98")



# Runs the script
if __name__ == "__main__":
	main()


