# Script to render a swarm plot with the speeds of the vehicles
# Author: Paul Goebel, 2035351
# Author: Kai Engelbart, 1270154

import psycopg2
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# Setup connection to DB
conn = psycopg2.connect("dbname='postgres' user='postgres' password='setpassword' host='localhost' port='5432'")

try:
    # Gather data from data mart
    query = "SELECT geschwindigkeit, fin FROM mart.f_fzg_messung INNER JOIN mart.d_fahrzeug ON mart.f_fzg_messung.d_fahrzeug_id = mart.d_fahrzeug.d_fahrzeug_id;"   
    df = pd.read_sql_query(query, conn)
    
    ax = sns.swarmplot(x="fin", y="geschwindigkeit", data=df)
    plt.savefig("Swarmy.png")
    
except (Exception, psycopg2.Error) as error:
    print("Error while rendering data: ", error)

finally:
    # closing database connection.
    if conn:
        conn.close()
