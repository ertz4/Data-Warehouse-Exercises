# Script to fill the Dimensions
# Author: Paul Goebel, 2035351

import psycopg2

# Setup connection to DB
conn = psycopg2.connect("dbname='postgres' user='postgres' password='setpassword' host='localhost' port='5432'")
cur = conn.cursor()

try:
    # Clean the d_ort dimension table and grab data from staging
    deletion_query = "DELETE FROM mart.d_ort;"
    query = "SELECT DISTINCT ort, land FROM staging.ort INNER JOIN staging.land ON staging.ort.land_id = staging.land.land_id;"

    cur.execute(deletion_query)
    cur.execute(query)
    res = cur.fetchall()

    # Insert the data into the dimension table
    for row in res:
        insert_query = "INSERT INTO mart.d_ort (ort, land) VALUES ('" + row[0] + "', '" + row[1] + "');"
        cur.execute(insert_query)

    # Clean the d_kunde dimension table and grab data from staging
    deletion_query = "DELETE FROM mart.d_kunde;"
    query = "SELECT DISTINCT kunde_id, vorname, nachname, anrede, geschlecht, geburtsdatum, wohnort, ort, land FROM staging.kunde INNER JOIN staging.ort ON staging.kunde.wohnort = staging.ort.ort_id INNER JOIN staging.land ON staging.ort.land_id = staging.land.land_id;"

    cur.execute(deletion_query)
    cur.execute(query)
    res = cur.fetchall()

    # Insert the data into the dimension table
    for row in res:
        insert_query = "INSERT INTO mart.d_kunde (kunde_id, vorname, nachname, anrede, geschlecht, geburtsdatum, wohnort_id, ort, land) VALUES (" + str(row[0]) + ", '" + row[1] + "', '" + row[2] + "', '" + row[3] + "', '" + row[4] + "', '" + str(row[5]) + "', " + str(row[6]) + ", '" + row[7] + "', '" + row[8] + "');"
        cur.execute(insert_query)
        
    # Clean the d_fahrzeug dimension table and grab data from staging
    deletion_query = "DELETE FROM mart.d_fahrzeug;"
    query = "SELECT DISTINCT staging.fahrzeug.fin, kfz_kennzeichen, baujahr, modell, hersteller FROM staging.fahrzeug INNER JOIN staging.kfzzuordnung ON staging.fahrzeug.fin = staging.kfzzuordnung.fin INNER JOIN staging.hersteller ON staging.fahrzeug.hersteller_code = staging.hersteller.hersteller_code;"

    cur.execute(deletion_query)
    cur.execute(query)
    res = cur.fetchall()

    # Insert the data into the dimension table
    for row in res:
        insert_query = "INSERT INTO mart.d_fahrzeug (fin, kfz_kennzeichen, baujahr, modell, hersteller) VALUES ('" + row[0] + "', '" + row[1] + "', " + str(row[2]) +", '" + row[3] +"', '" + row[4] +"');"
        cur.execute(insert_query)
        
    conn.commit()

except (Exception, psycopg2.Error) as error:
    print("Error while moving data from staging to mart", error)

finally:
    # closing database connection.
    if conn:
        cur.close()
        conn.close()
