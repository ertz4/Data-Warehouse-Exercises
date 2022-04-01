# Script to fill the Fact Tables
# Author: Paul Goebel, 2035351

import psycopg2

# Setup connection to DB
conn = psycopg2.connect("dbname='postgres' user='postgres' password='setpassword' host='localhost' port='5432'")
cur = conn.cursor()

try:
    
    
except (Exception, psycopg2.Error) as error:
    print("Error while moving data from staging to mart", error)

finally:
    # closing database connection.
    if conn:
        cur.close()
        conn.close()
