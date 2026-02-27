import time
import json
import random
from kafka import KafkaProducer

#Configuration Kafka
producer = KafkaProducer (
    bootstrap_servers ='localhost :9092 ',
    value_serializer = lambda v: json . dumps (v). encode ('utf -8 ')
)

def generate_sensor_data ():
     return {
        "sensor_id": " SENSOR_01 ",
        "timestamp": time . strftime ("%Y -%m -% dT%H:%M:% SZ"),
        "measurements": {
            "pm2_5": round ( random . uniform (5.0 , 50.0) , 2) ,
            "temp": round ( random . uniform (15.0 , 35.0) , 1) ,
            "humidity": random . randint (30 , 80)
        }
    }

if __name__ == "__main__":
    print (" Envoi des donnees vers Kafka ... ")
    while True :
        data = generate_sensor_data ()
        producer . send ('raw_data ', data )
        print (f" Envoye : { data }")
        time . sleep (2)   