from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession, SQLContext


# Run script by using:
# spark-submit --packages mysql:mysql-connector-java:5.1.39 [scriptname]


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
	
	# Just a print statement to see if the dataframe transferred sucessfully
	print promotion_df.show()

# Runs the script
if __name__ == "__main__":
	main()


