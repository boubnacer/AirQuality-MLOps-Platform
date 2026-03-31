import os
from dotenv import load_dotenv

load_dotenv ()

KAFKA_BOOTSTRAP_SERVERS = os. getenv ("KAFKA_BOOTSTRAP_SERVERS","localhost :29092")
SENSOR_COUNT = int (os. getenv ("SENSOR_COUNT","50"))
EMISSION_INTERVAL_SECONDS = float (os. getenv ("EMISSION_INTERVAL_SECONDS","1"))
KAFKA_TOPIC_RAW ="raw_data"
# Sensor locations (50 sensors across Europe )
SENSOR_LOCATIONS = [
    {"sensor_id": f"CAP_{str(i).zfill(3)}","lat": lat ,"lon": lon ,"city": city ,"zone": zone }
    for i, (lat , lon , city , zone ) in enumerate ([
        (48.8566 , 2.3522 ,"Paris","urban_background"),
        (48.8606 , 2.3376 ,"Paris","traffic"),
        (48.8738 , 2.2950 ,"Paris","industrial"),
        (51.5074 , -0.1278 ,"London","urban_background"),
        (51.5155 , -0.0922 ,"London","traffic"),
        (52.5200 , 13.4050 ,"Berlin","urban_background"),
        (52.4800 , 13.3900 ,"Berlin","residential"),
        (41.9028 , 12.4964 ,"Rome","urban_background"),
        (41.8860 , 12.5100 ,"Rome","traffic"),
        (40.4168 , -3.7038 ,"Madrid","urban_background"),
        (40.4300 , -3.6900 ,"Madrid","industrial"),
        (50.0755 , 14.4378 ,"Prague","urban_background"),
        (48.2082 , 16.3738 ,"Vienna","urban_background"),
        (47.3769 , 8.5417 ,"Zurich","residential"),
        (50.8503 , 4.3517 ,"Brussels","urban_background"),
        (52.3676 , 4.9041 ,"Amsterdam","traffic"),
        (55.6761 , 12.5683 ,"Copenhagen","residential"),
        (59.9139 , 10.7522 ,"Oslo","urban_background"),
        (60.1699 , 24.9384 ,"Helsinki","residential"),
        (59.3293 , 18.0686 ,"Stockholm","urban_background"),
        (48.1351 , 11.5820 ,"Munich","traffic"),
        (53.5753 , 10.0153 ,"Hamburg","industrial"),
        (51.2217 , 6.7762 ,"Dusseldorf","traffic"),
        (53.0793 , 8.8017 ,"Bremen","residential"),
        (45.4642 , 9.1900 ,"Milan","industrial"),
        (45.0703 , 7.6869 ,"Turin","traffic"),
        (43.7102 , 7.2620 ,"Nice","urban_background"),
        (43.2965 , 5.3698 ,"Marseille","industrial"),
        (45.7640 , 4.8357 ,"Lyon","traffic"),
        (47.2184 , -1.5536 ,"Nantes","residential"),
        (51.4545 , -2.5879 ,"Bristol","urban_background"),
        (53.4808 , -2.2426 ,"Manchester","industrial"),
        (53.8008 , -1.5491 ,"Leeds","traffic"),
        (55.8642 , -4.2518 ,"Glasgow","urban_background"),
        (57.1497 , -2.0943 ,"Aberdeen","residential"),
        (50.7184 , -3.5339 ,"Exeter","residential"),
        (52.4862 , -1.8904 ,"Birmingham","industrial"),
        (53.7676 , -0.3274 ,"Hull","industrial"),
        (51.4816 , -3.1791 ,"Cardiff","urban_background"),
        (54.5973 , -5.9301 ,"Belfast","urban_background"),
        (48.5734 , 7.7521 ,"Strasbourg","traffic"),
        (43.6047 , 1.4442 ,"Toulouse","residential"),
        (44.8378 , -0.5792 ,"Bordeaux","urban_background"),
        (49.4431 , 1.0993 ,"Rouen","industrial"),
        (50.6292 , 3.0573 ,"Lille","traffic"),
        (46.2276 , 2.2137 ,"Clermont","residential"),
        (48.6921 , 6.1844 ,"Nancy","urban_background"),
        (47.3220 , 5.0415 ,"Dijon","residential"),
        (45.1885 , 5.7245 ,"Grenoble","industrial"),
        (43.1242 , 5.9280 ,"Toulon","urban_background"),
    ], start =1)
]