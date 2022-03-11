# MQTT Subscriber
# Author: Paul Goebel, 2035351

import paho.mqtt.client as mqtt
import psycopg2
import json

# Setup connection to DB
conn = psycopg2.connect("dbname='postgres' user='postgres' password='setpassword' host='localhost' port='5432'")
cur = conn.cursor()

# Helper Function to check if a string is valid JSON
def validJSON(JSON):
    try:
        json.loads(JSON)
    except ValueError as err:
        return False
    return True

# Helper funtion called when messages are received
def on_message(client, userdata, message):
    print("message read: " + str(message.payload.decode("utf-8")))
    # Only write to database if JSON is valid
    if validJSON(str(message.payload.decode("utf-8"))):
        # Write JSON Message into the connected Database
        cur.execute("INSERT INTO staging.messung (payload, quelle) VALUES ('" + str(message.payload.decode("utf-8")) + "', 'MQTT');")
        conn.commit()
    else:
        print("Invalid JSON, Message skipped")

# Connect to Broker and subscribe to DataMgmt/FIN
broker_address="broker.hivemq.com"
client = mqtt.Client("607f5b68-d15c-45c9-b4b7-d312f3ca2a5e", clean_session=False)
client.on_message=on_message
client.connect(broker_address)
client.subscribe("DataMgmt/FIN", qos=1)
client.loop_forever()
