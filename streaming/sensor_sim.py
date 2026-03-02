import time
import json
import random

def generate_sensor_data ():
    """ Simule les donnees d’un capteur ( PDF Page 2)"""
    return {
        " sensor_id ": " SENSOR_01 ",
        " timestamp ": time . strftime ("%Y -%m -% dT%H:%M:% SZ"),
        " location ": {" lat": 48.85 , " lon ": 2.35} ,
        " measurements ": {
            " pm2_5 ": round ( random . uniform (5.0 , 50.0) , 2) ,
            " temp ": round ( random . uniform (15.0 , 35.0) , 1) ,
            " humidity ": random . randint (30 , 80)
        }
    }
if __name__ == "__main__":
    print (" Demarrage de la simulation ... ")
    while True :
        data = generate_sensor_data ()
        print (f" Donnees generees : { data }")
        # TODO : Prochaine etape -> Envoyer vers Kafka
        time . sleep (2)
