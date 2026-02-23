import pandas as pd
import numpy as np
import mlflow
import mlflow.sklearn
import matplotlib.pyplot as plt
from sklearn.ensemble import IsolationForest

np.random.seed(42)

# ================================
# 1. GENERATION DES DONNEES
# ================================
X_normal = pd.DataFrame({
    'pm2_5':       np.random.normal(20, 5, 100),
    'pm10':        np.random.normal(30, 7, 100),
    'no2':         np.random.normal(40, 8, 100),
    'o3':          np.random.normal(80, 10, 100),
    'temperature': np.random.normal(25, 2, 100),
    'humidity':    np.random.normal(65, 5, 100),
})

X_anomalies = pd.DataFrame({
    'pm2_5':       np.random.normal(100, 10, 10),
    'pm10':        np.random.normal(150, 15, 10),
    'no2':         np.random.normal(200, 20, 10),
    'o3':          np.random.normal(200, 20, 10),
    'temperature': np.random.normal(40, 3, 10),
    'humidity':    np.random.normal(90, 5, 10),
})

X = pd.concat([X_normal, X_anomalies], ignore_index=True)

# ================================
# 2. ENTRAINEMENT + MLFLOW
# ================================
mlflow.set_tracking_uri("http://localhost:5000")
mlflow.set_experiment("AirQuality_Anomaly")

print("Entrainement du modele...")
with mlflow.start_run(run_name="IsolationForest_V2"):

    clf = IsolationForest(contamination=0.1, random_state=42)
    clf.fit(X)

    predictions = clf.predict(X)
    n_anomalies = (predictions == -1).sum()

    # Paramètres et métriques
    mlflow.log_param("contamination", 0.1)
    mlflow.log_param("n_features", X.shape[1])
    mlflow.log_param("n_samples", len(X))
    mlflow.log_metric("anomalies_detected", int(n_anomalies))
    mlflow.log_metric("normal_detected", int((predictions == 1).sum()))

    # ================================
    # 3. GRAPHIQUE 1 : PM2.5 vs Temperature
    # ================================
    fig1, ax1 = plt.subplots(figsize=(10, 6))

    # Points normaux en bleu
    mask_normal = predictions == 1
    ax1.scatter(
        X[mask_normal]['pm2_5'],
        X[mask_normal]['temperature'],
        c='blue', label='Normal ✅', alpha=0.6, s=60
    )

    # Points anormaux en rouge
    mask_anomaly = predictions == -1
    ax1.scatter(
        X[mask_anomaly]['pm2_5'],
        X[mask_anomaly]['temperature'],
        c='red', label='Anomalie ⚠️', alpha=0.9, s=100, marker='X'
    )

    ax1.set_xlabel('PM2.5 (particules fines)', fontsize=12)
    ax1.set_ylabel('Température (°C)', fontsize=12)
    ax1.set_title('Détection d\'Anomalies - PM2.5 vs Température', fontsize=14)
    ax1.legend(fontsize=11)
    ax1.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig('graphique_anomalies.png', dpi=150)
    plt.show()

    # ================================
    # 4. GRAPHIQUE 2 : Toutes les métriques
    # ================================
    fig2, axes = plt.subplots(2, 3, figsize=(15, 8))
    fig2.suptitle('Vue Globale - Toutes les Mesures', fontsize=16)

    colonnes = ['pm2_5', 'pm10', 'no2', 'o3', 'temperature', 'humidity']
    titres   = ['PM2.5', 'PM10', 'NO2', 'O3', 'Température', 'Humidité']

    for i, (col, titre) in enumerate(zip(colonnes, titres)):
        ax = axes[i//3][i%3]

        ax.scatter(
            range(len(X[mask_normal])),
            X[mask_normal][col],
            c='blue', alpha=0.5, s=30, label='Normal'
        )
        ax.scatter(
            range(len(X[mask_anomaly])),
            X[mask_anomaly][col],
            c='red', alpha=0.9, s=80, marker='X', label='Anomalie'
        )

        ax.set_title(titre, fontsize=11)
        ax.grid(True, alpha=0.3)
        ax.legend(fontsize=8)

    plt.tight_layout()
    plt.savefig('graphique_global.png', dpi=150)
    plt.show()

    # Sauvegarde des graphiques dans MLflow
    mlflow.log_artifact('graphique_anomalies.png')
    mlflow.log_artifact('graphique_global.png')

    # Sauvegarde du modèle
    mlflow.sklearn.log_model(clf, "model_isolation_forest")

    print(f"")
    print(f"✅ Modele enregistre avec succes !")
    print(f"⚠️  Anomalies detectees : {n_anomalies}")
    print(f"✅ Mesures normales     : {(predictions == 1).sum()}")
    print(f"📊 Graphiques sauvegardes dans MLflow !")