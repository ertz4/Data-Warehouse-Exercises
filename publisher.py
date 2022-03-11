# MQTT Publisher
# Author: Paul Goebel, 2035351

import paho.mqtt.client as mqtt

from random import randrange
from datetime import datetime
from time import sleep

# Connect to Broker
broker_address="broker.hivemq.com"
client = mqtt.Client("2aeb92c4-c13f-4687-9c9c-cc1ee5c5b0af", clean_session=False)
client.connect(broker_address)

# Continously generate messages
while True:
    # Generate JSON message 
    time = datetime.now()
    speed = randrange(201)
    message = '{"fin":"WVW345TH6M9566671","zeit":"' + time.strftime("%d.%m.%Y %H:%M:%S.%f")[:-3] + '","geschwindigkeit":' + str(speed) + ',"ort":4}'

    # Send generated Message to Broker
    client.publish("DataMgmt/FIN", message, qos=1)
    print("message sent: " + message)
    
    # Wait 5 seconds
    sleep(5)
