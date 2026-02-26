from kafka import KafkaConsumer
import json
import pandas as pd
import mlflow.sklearn

# 1. Charger le modele depuis MLflow
print("Chargement du modele...")
model_uri = "models:/IsolationForest/Production"
# model = mlflow.sklearn.load_model(model_uri)

# 2. Se connecter a Kafka
print("Connexion a Kafka...")
consumer = KafkaConsumer(
    'raw_data',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Ecoute des messages en cours...")
for message in consumer:
    data = message.value

    # Preparer la donnee pour le modele
    df = pd.DataFrame([data['measurements']])

    # Predire
    # prediction = model.predict(df)
    print(f"Analyse de : {data['sensor_id']} a {data['timestamp']}")

    # if prediction[0] == -1:
    #     print("!!! ALERTE ANOMALIE DETECTEE !!!")
    # else:
    #     print("OK - Donnee normale")