# Queries from snowflake
import snowflake.connector
import logging
import os


logging.basicConfig(
    filename='/tmp/snowflake_query.log',
    level=logging.INFO)

#Main function
def main():
    ACCOUNT = 'md17171'
    USER = 'BHTraining1219'
    PASSWORD = 'Groupc1219'

    con = snowflake.connector.connect(
        user=USER,
        password=PASSWORD,
        account=ACCOUNT,
    )

    con.cursor().execute("USE DATABASE FOODMART")
    con.cursor().execute("USE WAREHOUSE COMPUTE_WH")
    con.cursor().execute("USE FOODMART.PUBLIC")

    q1 = con.cursor().execute("""
    SELECT "REGION_ID", "PROMOTION_ID", "COST", "WEEKDAY_SALES" as "TOTAL WEEKDAY SALES", "WEEKEND_SALES" as "TOTAL WEEKEND SALES" FROM "FOODMART"."PUBLIC"."AGGREGATESALES" ORDER BY "REGION_ID"
    """)

    
    con.cursor().execute("""
    SELECT "REGION_ID", "PROMOTION_ID", "COST", "WEEKDAY_SALES" + "WEEKEND_SALES" as "TOTAL SALES" FROM "FOODMART"."PUBLIC"."AGGREGATESALES" ORDER BY "TOTAL SALES" DESC
    """)




# Runs the script
if __name__ == "__main__":
	main()
