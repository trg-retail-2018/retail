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
    CREATE OR REPLACE TABLE TOTAL_SALES AS
    SELECT "REGION_ID", "PROMOTION_ID", "COST", IFNULL("WEEKDAY_SALES",0) as "TOTAL WEEKDAY SALES", IFNULL("WEEKEND_SALES",0) as "TOTAL WEEKEND SALES" FROM "FOODMART"."PUBLIC"."AGGREGATESALES" ORDER BY "REGION_ID"
    """)


    q2 = con.cursor().execute("""
    CREATE OR REPLACE TABLE BEST_PROMOTIONS AS
    SELECT t1.REGION_ID, t1.PROMOTION_ID, t1.COST, (IFNULL(t1.WEEKDAY_SALES, 0) + IFNULL(t1.WEEKEND_SALES, 0)) as "TOTAL_SALES"
    FROM "FOODMART"."PUBLIC"."AGGREGATESALES" as t1
    JOIN
        (SELECT "REGION_ID", max(IFNULL("WEEKDAY_SALES", 0) + IFNULL("WEEKEND_SALES", 0)) as "TOTAL_SALES"
         from "FOODMART"."PUBLIC"."AGGREGATESALES" GROUP BY "REGION_ID") as t2
    ON t1.REGION_ID = t2.REGION_ID
    AND (IFNULL(t1.WEEKDAY_SALES, 0) + IFNULL(t1.WEEKEND_SALES, 0)) = t2.TOTAL_SALES
    """)






# Runs the script
if __name__ == "__main__":
	main()
