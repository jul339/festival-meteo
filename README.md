# Festival Météo - Agrégation de données avec PySpark

Projet d'agrégation de données festivals (SIBIL) et météo (Météo-France) avec PySpark pour analyser les conditions météorologiques lors des événements culturels.

## Sources de données OpenData

1. **Données SIBIL** : Export CSV des festivals et événements culturels
   - Format : CSV avec séparateur `;`
   - Colonnes : festival_nom, lieu_adresse, lieu_ville, declaration_date_representation, etc.

2. **API Météo-France** : Données climatologiques quotidiennes
   - Endpoint : `https://public-api.meteofrance.fr/public/DPClim/v1`
   - Authentification : Token OAuth2

3. **API Géoplateforme** : Géocodage d'adresses
   - Endpoint : `https://data.geopf.fr/geocodage`
   - Service : Conversion adresse → coordonnées (latitude, longitude)

## Architecture des données

### Table : `sibil_events`
Événements festivals extraits du CSV SIBIL
- `festival_nom`, `lieu_nom`, `lieu_adresse`, `lieu_ville`
- `lieu_departement_code`, `declaration_date_representation`
- `latitude`, `longitude` (ajoutées via géocodage)

### Table : `meteo_data`
Données météo quotidiennes par station
- `station_id`, `date`
- `temperature`, `precipitation`, `humidite`
- Autres paramètres climatologiques

### Table : `sibil_meteo_aggregated`
Agrégation finale : événements SIBIL + données météo quotidiennes
- Toutes les colonnes SIBIL
- Données météo pour chaque jour de la période de l'événement
- Une ligne = un jour de météo pour un événement

## Installation

```bash
# Prérequis : Java 11+ et Python 3.9-3.10
uv venv
source .venv/bin/activate
uv pip install -e .
```

Créer un fichier `.env` :
```
TOK=votre_token_meteo_france
GEOPF_TOKEN=votre_token_geoplateforme
```

## Processus PySpark d'import et enregistrement

### Étapes du processus

1. **Extraction SIBIL** (`src/sibil_manipulation.py`)
   - Lecture du CSV avec schéma explicite
   - Filtrage selon critères (département, festival, etc.)

2. **Géocodage** (`src/get_coord_API.py`)
   - Construction des adresses complètes
   - Appel API batch pour obtenir coordonnées (lat, lon)

3. **Récupération météo** (`src/extract_meteo_API.py`)
   - Recherche de la station météo la plus proche
   - Commande et récupération des données quotidiennes via API

4. **Agrégation** (`src/aggregation.py`)
   - Jointure SIBIL + météo par coordonnées et dates
   - Création du DataFrame final agrégé

### Exécution

```bash
python src/aggregation.py
```

## Stockage

Les données agrégées sont stockées au format **Parquet** (format optimisé pour Spark) :

```python
df_result.write.mode("overwrite").parquet("data/processed/sibil_meteo_aggregated.parquet")
```

Le format Parquet permet :
- Compression efficace
- Lecture rapide avec Spark
- Compatibilité avec HDFS

## Exemple d'utilisation

```python
from src.aggregation import SIBILMeteoAggregator
import os
from dotenv import load_dotenv

load_dotenv()

with SIBILMeteoAggregator(
    meteo_token=os.getenv("TOK"),
    geocodage_token=os.getenv("GEOPF_TOKEN")
) as aggregator:
    df_result = aggregator.aggregate_sibil_meteo(
        csv_path="data/raw_sibil/Export_SIBIL_dataculture.csv",
        filters={"lieu_departement_code": 31}
    )
    
    df_result.write.mode("overwrite").parquet("data/processed/result.parquet")
```
