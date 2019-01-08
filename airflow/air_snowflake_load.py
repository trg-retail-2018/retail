# Loads data from s3 into snowflake
import snowflake.connector
import logging
import os

logging.basicConfig(
    filename='/tmp/snowflake_python_connector.log',
    level=logging.INFO)



#Main function
def main():

    # Path to the source bucket
    WHERE_DATA_IS = "s3://ashicurated/foodmart/aggregate/"

    ACCOUNT = 'md17171'
    USER = 'BHTraining1219'
    PASSWORD = 'Groupc1219'

    # Get keys from environmental variables
    AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')

    con = snowflake.connector.connect(
        user=USER,
        password=PASSWORD,
        account=ACCOUNT,
    )

    con.cursor().execute("CREATE DATABASE IF NOT EXISTS FOODMART")

    con.cursor().execute("USE DATABASE FOODMART")
    con.cursor().execute("USE WAREHOUSE COMPUTE_WH")
    con.cursor().execute("USE FOODMART.PUBLIC")

    # Not necessary but it was in the tutorial
    con.cursor().execute("CREATE OR REPLACE FILE FORMAT mycsv TYPE='CSV' FIELD_DELIMITER = ','")

    # Create the table
    con.cursor().execute(
    "CREATE OR REPLACE TABLE "
    "AGGREGATESALES(promotion_id integer, region_id integer, the_year string, the_month string, cost decimal, weekday_sales decimal, weekend_sales decimal)")


    # Copying Data
    con.cursor().execute("""
    COPY INTO FOODMART.PUBLIC.AGGREGATESALES FROM {bucket_src_path}
        CREDENTIALS = (
            aws_key_id='{aws_access_key_id}',
            aws_secret_key='{aws_secret_access_key}')
        FILE_FORMAT=(format_name = mycsv)
        force = true
    """.format(
        bucket_src_path=WHERE_DATA_IS,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY))

    con.cursor().close()



# Runs the script
if __name__ == "__main__":
	main()
