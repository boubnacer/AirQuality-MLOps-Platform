import streamlit as st
import pandas as pd
import numpy as np

# Configuration de la page
st.set_page_config(page_title="Qualite de l’Air - MLOps", layout="wide")
st.title("Tableau de Bord : Detection d’Anomalies (Qualite de l’Air)")

# 1. Generation de fausses donnees (temporaire)
st.sidebar.header("Parametres")
st.sidebar.info("La connexion a PostgreSQL sera ajoutee plus tard.")

# On simule 50 points de donnees
dates = pd.date_range(end=pd.Timestamp.now(), periods=50, freq='H')
data = pd.DataFrame({
    'Temps': dates,
    'Temperature': np.random.normal(25, 2, 50),
    'PM2_5': np.random.normal(20, 5, 50)
})

# 2. Affichage des graphiques
st.subheader("Evolution de la Temperature dans le temps")
st.line_chart(data.set_index('Temps')['Temperature'])

st.subheader("Concentration de PM2.5")
st.line_chart(data.set_index('Temps')['PM2_5'])

st.success("Interface chargee avec succes !")