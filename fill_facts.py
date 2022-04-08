# Script to fill the Fact Tables
# Author: Paul Goebel, 2035351

import psycopg2
import json

# Setup connection to DB
conn = psycopg2.connect("dbname='postgres' user='postgres' password='setpassword' host='localhost' port='5432'")
cur = conn.cursor()

try:
    # Gather data from staging and dimension tables, format it where needed and insert into fact table
    query = "INSERT INTO mart.f_fzg_messung (messung_erzeugt, empfang_eingetroffen, d_kunde_id, d_fahrzeug_id, d_ort_id, geschwindigkeit) SELECT TO_TIMESTAMP( payload ->> 'zeit', 'DD.MM.YYYY HH24:MI:SS.MS'), staging.messung.erstellt_am, mart.d_kunde.d_kunde_id, mart.d_fahrzeug.d_fahrzeug_id, mart.d_ort.d_ort_id, CAST(payload ->> 'geschwindigkeit' AS INTEGER) FROM staging.messung INNER JOIN staging.fahrzeug ON staging.messung.payload ->> 'fin' = staging.fahrzeug.fin INNER JOIN staging.kunde ON staging.fahrzeug.kunde_id = staging.kunde.kunde_id INNER JOIN mart.d_kunde ON staging.fahrzeug.kunde_id = mart.d_kunde.kunde_id INNER JOIN mart.d_fahrzeug ON staging.fahrzeug.fin = mart.d_fahrzeug.fin INNER JOIN staging.ort ON CAST(staging.messung.payload ->> 'ort' AS INTEGER) = staging.ort.ort_id INNER JOIN mart.d_ort ON staging.ort.ort = mart.d_ort.ort;"
    cur.execute(query)    
    conn.commit()
    
except (Exception, psycopg2.Error) as error:
    print("Error while moving data from staging to mart: ", error)

finally:
    # closing database connection.
    if conn:
        cur.close()
        conn.close()
