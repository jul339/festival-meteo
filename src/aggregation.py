from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, concat_ws, lit, when, isnan, isnull, udf, explode, array, struct
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, ArrayType
from typing import Dict, List, Optional, Tuple, Any
import os
import time
import csv
import io
from datetime import datetime, timedelta

from dotenv import load_dotenv
load_dotenv()

from sibil_manipulation import SIBILExtractor
from get_coord_API import get_coordinates_from_addresses_batch
from extract_meteo_API import (
    get_liste_stations_quotidienne,
    get_information_station,
    command_station_data_quotidienne,
    get_csv_from_command_id
)


class SIBILMeteoAggregator:
    """
    Classe pour agréger les données SIBIL avec les données météo.
    Récupère les événements SIBIL, trouve les coordonnées des adresses,
    et récupère les données météo pour chaque événement.
    """
    
    def __init__(self, meteo_token: str, geocodage_token: Optional[str] = None):
        """
        Initialise l'agrégateur.
    
    Args:
            meteo_token: Token d'authentification pour l'API Météo-France
            geocodage_token: Token optionnel pour l'API de géocodage
        """
        self.meteo_token = meteo_token
        self.geocodage_token = geocodage_token
        self.extractor = SIBILExtractor()
    
    def __enter__(self):
        """Support pour le context manager."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Arrête Spark à la sortie du context manager."""
        self.extractor.stop()
    
    def _build_address_udf(self):
        """
        Crée une UDF Spark pour construire une adresse complète.
        
        Returns:
            UDF Spark
        """
        def build_address(adresse: str, code_postal: str, ville: str) -> str:
            """Construit une adresse complète."""
            address_parts = []
            
            if adresse and str(adresse).strip() not in ["null", "None", "", None]:
                address_parts.append(str(adresse).strip())
            if code_postal and str(code_postal).strip() not in ["null", "None", "", None]:
                address_parts.append(str(code_postal).strip())
            if ville and str(ville).strip() not in ["null", "None", "", None]:
                address_parts.append(str(ville).strip())
            
            return ", ".join(address_parts) if address_parts else None
        
        return udf(build_address, StringType())
    
    def _get_coordinates_udf(self):
        """
        Crée une UDF Spark pour récupérer les coordonnées d'une adresse.
        Note: Cette UDF fait des appels API, donc elle peut être lente.
        Pour de meilleures performances, on peut pré-géocoder les adresses uniques.
        
        Returns:
            UDF Spark qui retourne un struct avec latitude et longitude
        """
        def get_coords(address: str) -> Optional[Tuple[float, float]]:
            """Récupère les coordonnées d'une adresse."""
            if not address or address in ["null", "None", ""]:
                return None
            
            try:
                coords = get_coordinates_from_addresses_batch(
                    [address], 
                    token=self.geocodage_token
                )
                if coords and coords[0]:
                    return coords[0]
                return None
            except Exception as e:
                print(f"Erreur lors du géocodage de {address}: {e}")
                return None
        
        coord_schema = StructType([
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True)
        ])
        
        return udf(get_coords, coord_schema)
    
    def _get_coordinates_for_df(self, df: DataFrame) -> DataFrame:
        """
        Ajoute les coordonnées (latitude, longitude) au DataFrame SIBIL.
        Utilise une approche optimisée avec Spark pour gérer les adresses uniques.
        
        Args:
            df: DataFrame Spark avec les données SIBIL
        
        Returns:
            DataFrame avec les colonnes latitude et longitude ajoutées
        """
        # Construire l'adresse avec une UDF
        build_address = self._build_address_udf()
        df_with_address = df.withColumn(
            "full_address",
            build_address(col("lieu_adresse"), col("lieu_code_postal"), col("lieu_ville"))
        )
        
        # Pour optimiser, on peut géocoder uniquement les adresses uniques
        # puis faire un join
        unique_addresses_df = df_with_address.select("full_address").distinct()
        
        print(f"Géocodage de {unique_addresses_df.count()} adresses uniques...")
        
        # Collecter les adresses uniques pour le géocodage batch
        unique_addresses = [row.full_address for row in unique_addresses_df.collect() if row.full_address]
        
        if not unique_addresses:
            return df_with_address.withColumn("latitude", lit(None).cast(DoubleType())) \
                                  .withColumn("longitude", lit(None).cast(DoubleType()))
        
        # Géocoder par batch (plus efficace)
        print(f"Géocodage batch de {len(unique_addresses)} adresses...")
        coordinates = get_coordinates_from_addresses_batch(
            unique_addresses,
            token=self.geocodage_token
        )
        
        # Créer un DataFrame de mapping adresse -> coordonnées
        coord_mapping_data = []
        for addr, coord in zip(unique_addresses, coordinates):
            if coord:
                coord_mapping_data.append({
                    "full_address": addr,
                    "latitude": coord[0],
                    "longitude": coord[1]
                })
            else:
                coord_mapping_data.append({
                    "full_address": addr,
                    "latitude": None,
                    "longitude": None
                })
        
        coord_mapping_df = self.extractor.spark.createDataFrame(coord_mapping_data)
        
        # Joindre avec le DataFrame original
        df_with_coords = df_with_address.join(
            coord_mapping_df,
            on="full_address",
            how="left"
        ).drop("full_address")
        
        return df_with_coords
    
    def _find_station(
        self, 
        latitude: float, 
        longitude: float, 
        departement_code: int,
        date_deb: str,
        date_fin: str
    ) -> Optional[int]:
        """
        Trouve la station météo la plus proche pour des coordonnées données.
        
        Args:
            latitude: Latitude
            longitude: Longitude
            departement_code: Code du département
        
        Returns:
            ID de la station la plus proche ou None
        """
        try:
            # Récupérer la liste des stations du département
            stations_data = get_liste_stations_quotidienne(
                id_departement=departement_code,
                token=self.meteo_token
            )
            
            if not stations_data and "data" not in stations_data:
                return None
            if "data" in stations_data:
                stations = stations_data["data"]
                if not stations:
                    return None
            else:
                stations = stations_data
            
            # Calculer la distance pour chaque station et trouver la plus proche
            min_distance = float('inf')
            nearest_station_id = None
            
            for station in stations:
                if station.get("posteOuvert") == False:
                    continue
                try:
                    station_id = station.get("id")
                    if not station_id:
                        continue
                    
                    # Les coordonnées sont directement disponibles dans les données de la station
                    station_lat = station.get("lat")
                    station_lon = station.get("lon")
                    
                    if station_lat is not None and station_lon is not None:
                        # Calculer la distance (formule de Haversine simplifiée)
                        distance = ((latitude - station_lat) ** 2 + 
                                   (longitude - station_lon) ** 2) ** 0.5
                        
                        if distance < min_distance:
                            
                            info_station = get_information_station(station_id, self.meteo_token)[0]
                            if info_station:
                                start_date_meteo_dt = datetime.strptime(info_station.get("dateDebut"), "%Y-%m-%d %H:%M:%S")
                                end_date_meteo_dt = info_station.get("dateFin")
                                if end_date_meteo_dt == '' or end_date_meteo_dt == None:
                                    end_date_meteo_dt = datetime.now()
                                else:
                                    end_date_meteo_dt = datetime.strptime(end_date_meteo_dt, "%Y-%m-%d %H:%M:%S") 
                                if start_date_meteo_dt  <= datetime.strptime(date_deb, "%Y-%m-%d") and end_date_meteo_dt >= datetime.strptime(date_fin, "%Y-%m-%d"):
                                    min_distance = distance
                                    nearest_station_id = station_id
                        else:
                            continue

                except Exception as e:
                    print(f"Erreur lors du traitement de la station {station_id}: {e}")
                    continue
            
            return nearest_station_id
            
        except Exception as e:
            print(f"Erreur lors de la recherche de la station la plus proche: {e}")
            return None
    
    def _get_weather_data(
        self, 
        station_id: int, 
        date_debut: str, 
        date_fin: str
    ) -> Optional[DataFrame]:
        """
        Récupère les données météo pour une station et une période données.
        
        Args:
            station_id: ID de la station météo
            date_debut: Date de début au format YYYY-MM-DD
            date_fin: Date de fin au format YYYY-MM-DD
        
        Returns:
            DataFrame Spark avec les données météo ou None
        """
        try:
            # Commander les données météo
            command_response = command_station_data_quotidienne(
                id_station=station_id,
                date_deb_periode=date_debut,
                date_fin_periode=date_fin,
                token=self.meteo_token
            )
            
            # Extraire l'ID de commande de la réponse
            # Structure: {'elaboreProduitAvecDemandeResponse': {'return': '2026000713302'}}
            if not command_response:
                print(f"Erreur: réponse vide")
                return None
            
            try:
                command_id = command_response.get('elaboreProduitAvecDemandeResponse', {}).get('return')
                if not command_id:
                    print(f"Erreur: pas d'ID de commande dans la réponse")
                    print(f"Structure de la réponse: {command_response}")
                    return None
            except (KeyError, AttributeError) as e:
                print(f"Erreur lors de l'extraction de l'ID de commande: {e}")
                print(f"Structure de la réponse: {command_response}")
                return None
            
            # Attendre que la commande soit prête (polling)
            max_attempts = 5
            attempt = 0
            while attempt < max_attempts:
                time.sleep(2)  # Attendre 2 secondes entre chaque tentative
                try:
                    csv_data = get_csv_from_command_id(command_id, self.meteo_token)
                    if csv_data and len(csv_data) > 0:
                        # Parser le CSV et créer un DataFrame
                        csv_reader = csv.DictReader(io.StringIO(csv_data))
                        rows = list(csv_reader)
                        
                        if rows:
                            # Nettoyer les données : remplacer None par des chaînes vides
                            cleaned_rows = []
                            for row in rows:
                                cleaned_row = {}
                                for key, value in row.items():
                                    # Remplacer None, 'None', ou chaînes vides par une chaîne vide
                                    if value is None or value == 'None' or value == '':
                                        cleaned_row[key] = ''
                                    else:
                                        cleaned_row[key] = str(value)  # S'assurer que tout est une string
                                cleaned_rows.append(cleaned_row)
                            
                            # Créer un DataFrame Spark avec les données nettoyées
                            meteo_df = self.extractor.spark.createDataFrame(cleaned_rows)
                            return meteo_df

                except Exception as e:
                    # La commande n'est peut-être pas encore prête
                    if "404" in str(e) or "not found" in str(e).lower():
                        attempt += 1
                        continue
                    else:
                        print(f"Erreur lors de la récupération du CSV: {e}")
                        return None
                
                attempt += 1
            
            print(f"Timeout: la commande {command_id} n'est pas prête après {max_attempts} tentatives")
            return None
            
        except Exception as e:
            print(f"Erreur lors de la récupération des données météo: {e}")
            return None
    
    def aggregate_sibil_meteo(
        self,
        csv_path: str,
        filters: Dict[str, Any] = None,
        sibil_columns: List[str] = None,
        date_debut: Optional[str] = None,
        date_fin: Optional[str] = None
    ) -> DataFrame:
        """
        Agrège les données SIBIL avec les données météo.
    
    Args:
            csv_path: Chemin vers le fichier CSV SIBIL
            filters: Dictionnaire de filtres pour SIBIL {colonne: valeur}
            sibil_columns: Liste de colonnes SIBIL à conserver
            date_debut: Date de début pour les données météo (YYYY-MM-DD). 
                       Si None, utilise la date de représentation de la déclaration
            date_fin: Date de fin pour les données météo (YYYY-MM-DD).
                     Si None, utilise date_debut + 7 jours
        
        Returns:
            DataFrame Spark avec les données SIBIL et météo agrégées
        """
        # Colonnes SIBIL par défaut si non spécifiées
        if sibil_columns is None:
            sibil_columns = [
                "festival_nom", "lieu_nom", "lieu_adresse", "lieu_code_postal", 
                "lieu_ville", "lieu_departement_code", "declaration_date_representation"
            ]
        
        # Ajouter les colonnes nécessaires pour construire l'adresse
        required_columns = ["lieu_adresse", "lieu_code_postal", "lieu_ville", "lieu_departement_code"]
        for col_name in required_columns:
            if col_name not in sibil_columns:
                sibil_columns.append(col_name)
        
        # Récupérer les données SIBIL filtrées
        print("Extraction des données SIBIL...")
        df_sibil = self.extractor.extract_SIBIL_filtered(
            csv_path=csv_path,
            filters=filters,
            columns=sibil_columns
        )
        
        # Filtrer les lignes avec des adresses valides
        df_sibil = df_sibil.filter(
            (col("lieu_adresse").isNotNull()) & 
            (col("lieu_adresse") != "null") &
            (col("lieu_ville").isNotNull()) &
            (col("lieu_ville") != "null")
        )
        
        # Ajouter les coordonnées
        print("Ajout des coordonnées géographiques...")
        df_with_coords = self._get_coordinates_for_df(df_sibil)
        
        # Filtrer les lignes avec des coordonnées valides
        df_with_coords = df_with_coords.filter(
            col("latitude").isNotNull() & col("longitude").isNotNull()
        )
        
        # Ajouter une colonne pour calculer les dates de début et fin
        def parse_date_udf():
            """UDF pour parser les dates."""
            def parse_date(date_str: str, default_date: str) -> str:
                if not date_str or str(date_str) in ["null", "None", ""]:
                    return default_date
                date_str = str(date_str).split()[0]
                for fmt in ["%Y-%m-%d", "%Y/%m/%d", "%d/%m/%Y", "%d-%m-%Y"]:
                    try:
                        return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
                    except:
                        continue
                return default_date
            return udf(parse_date, StringType())
        
        # Calculer les dates de début et fin
        default_date_deb = date_debut if date_debut else (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
        default_date_fin = date_fin if date_fin else datetime.now().strftime("%Y-%m-%d")
        
        parse_date = parse_date_udf()
        df_with_dates = df_with_coords.withColumn(
            "date_debut_meteo",
            when(
                lit(date_debut).isNotNull(),
                lit(date_debut)
            ).otherwise(
                when(
                    col("declaration_date_representation").isNotNull(),
                    parse_date(col("declaration_date_representation"), lit(default_date_deb))
                ).otherwise(lit(default_date_deb))
            )
        ).withColumn(
            "date_fin_meteo",
            when(
                lit(date_fin).isNotNull(),
                lit(date_fin)
            ).otherwise(
                when(
                    col("date_debut_meteo").isNotNull(),
                    (col("date_debut_meteo").cast("date") + lit(7).cast("int")).cast("string")
                ).otherwise(lit(default_date_fin))
            )
        )
        
        # Filtrer les lignes avec coordonnées valides
        df_valid = df_with_dates.filter(
            col("latitude").isNotNull() & 
            col("longitude").isNotNull() &
            col("lieu_departement_code").isNotNull()
        )
        
        print(f"Traitement de {df_valid.count()} événements SIBIL avec données météo...")
        
        # Pour optimiser, on va traiter par groupes uniques (station, dates)
        # On va collecter les combinaisons uniques (lat, lon, dept, date_deb, date_fin) et traiter par batch
        # Cela évite de récupérer plusieurs fois les mêmes données météo
        
        # Créer un identifiant unique pour chaque combinaison station/date
        df_with_station_key = df_valid.withColumn(
            "station_key",
            concat_ws("_",
                col("latitude").cast("string"),
                col("longitude").cast("string"),
                col("lieu_departement_code").cast("string"),
                col("date_debut_meteo"),
                col("date_fin_meteo")
            )
        )
        
        # Collecter les combinaisons uniques pour optimiser les appels API
        unique_combinations = df_with_station_key.select(
            "station_key", "latitude", "longitude", "lieu_departement_code", 
            "date_debut_meteo", "date_fin_meteo"
        ).distinct().collect()
        
        print(f"Traitement de {len(unique_combinations)} combinaisons uniques station/date...")
        
        # Dictionnaire pour stocker les données météo par clé
        meteo_data_cache = {}
        
        for idx, combo in enumerate(unique_combinations, 1):
            print(f"\nTraitement de la combinaison {idx}/{len(unique_combinations)}...")
            lat = combo.latitude
            lon = combo.longitude
            dept = combo.lieu_departement_code
            date_deb = combo.date_debut_meteo
            date_f = combo.date_fin_meteo
            key = combo.station_key
            
            # Trouver la station
            print(f"  → Recherche station pour ({lat}, {lon}) dans le département {dept}...")
            station_id = self._find_station(float(lat), float(lon), int(dept), date_deb, date_f)
            
            if not station_id:
                print(f"  → Aucune station trouvée")
                continue
            
            # Récupérer les données météo
            print(f"  → Station: {station_id}, Période: {date_deb} à {date_f}")
            meteo_df = self._get_weather_data(station_id, date_deb, date_f)
            
            if meteo_df:
                meteo_data_cache[key] = (station_id, meteo_df)
                print(f"  → {meteo_df.count()} jours de données météo récupérées")
            else:
                meteo_data_cache[key] = (station_id, None)
                print(f"  → Aucune donnée météo disponible")
            
            time.sleep(1)  # Délai pour éviter de surcharger les APIs
        
        # Maintenant, joindre les données météo avec le DataFrame SIBIL
        # Pour chaque combinaison unique, on va créer un DataFrame avec les données météo
        all_meteo_dfs = []
        
        for key, (station_id, meteo_df) in meteo_data_cache.items():
            if meteo_df:
                # Ajouter la clé et l'ID de station aux données météo
                meteo_with_key = meteo_df.withColumn("station_key", lit(key)) \
                                         .withColumn("station_id", lit(station_id))
                all_meteo_dfs.append(meteo_with_key)
        
        if all_meteo_dfs:
            # Union de tous les DataFrames météo
            from functools import reduce
            meteo_union = reduce(DataFrame.unionByName, all_meteo_dfs)
            
            # Joindre avec le DataFrame SIBIL
            final_df = df_with_station_key.join(
                meteo_union,
                on="station_key",
                how="left"
            ).drop("station_key")
        else:
            # Pas de données météo, juste ajouter station_id
            final_df = df_with_station_key.withColumn("station_id", lit(None).cast(IntegerType())) \
                                          .drop("station_key")
        
        return final_df


# Exemple d'utilisation
if __name__ == "__main__":
    import os
    from dotenv import load_dotenv
    load_dotenv()
    
    csv_path = "data/raw_sibil/Export_SIBIL_dataculture.csv"
    meteo_token = os.getenv("TOK")
    geocodage_token = os.getenv("GEOPF_TOKEN")  # Optionnel
    
    if not meteo_token:
        raise ValueError("METEO_FRANCE_TOKEN doit être défini dans le fichier .env")
    
    # Utiliser l'agrégateur avec context manager
    with SIBILMeteoAggregator(meteo_token=meteo_token, geocodage_token=geocodage_token) as aggregator:
        # Agrégation avec filtres
        df_result = aggregator.aggregate_sibil_meteo(
            csv_path=csv_path,
            filters={"festival_nom": "monatgne et nature "},  # Exemple: Haute-Garonne
            sibil_columns=[
                "festival_nom", "lieu_nom", "lieu_adresse", 
                "lieu_code_postal", "lieu_ville", "lieu_departement_code",
                "declaration_date_representation"
            ],
        )
        
        print(f"Nombre de lignes dans le résultat: {df_result.count()}")
        df_result.show(20, truncate=False)
        
        # Sauvegarder le résultat
        df_result.write.mode("overwrite").parquet("data/processed/sibil_meteo_aggregated.parquet")
