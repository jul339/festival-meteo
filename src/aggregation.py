from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, concat_ws, lit, trim, udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, ArrayType
from typing import Dict, List, Optional, Tuple, Any
import os
import time
import csv
import io
from datetime import datetime, timedelta

from dotenv import load_dotenv

from sibil_manipulation import SIBILExtractor
from get_coord_API import get_coordinates_from_addresses_batch
from extract_meteo_API import _get_weather_data, _find_station


class SIBILMeteoAggregator:
    """
    Classe pour agréger les données SIBIL avec les données météo.
    Récupère les événements SIBIL, trouve les coordonnées des adresses,
    et récupère les données météo pour chaque événement.
    """
    
    def __init__(self, meteo_token: str, geocodage_token: Optional[str] = None):
        self.meteo_token = meteo_token
        self.geocodage_token = geocodage_token
        
        # Créer la session Spark centralisée
        self.spark = SparkSession.builder \
            .appName("SIBIL_Meteo_Aggregation") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        self.extractor = SIBILExtractor(spark=self.spark)

    def __enter__(self):
        """Support pour le context manager."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Arrête Spark à la sortie du context manager."""
        self.extractor.stop()
    

    
    def _get_coordinates_for_df(self, unique_addresses: List[str]) -> list[str]:
        """
        Ajoute les coordonnées (latitude, longitude) au DataFrame SIBIL.
        Utilise une approche optimisée avec Spark pour gérer les adresses uniques.
        
        Args:
            df: DataFrame Spark avec les données SIBIL
        
        Returns:
            DataFrame avec les colonnes latitude et longitude ajoutées
        """

        
        if not unique_addresses:
            raise ValueError("Aucune adresse trouvée")

        print(f"Géocodage batch de {len(unique_addresses)} adresses...")

        coordinates = get_coordinates_from_addresses_batch(
            unique_addresses,
            token=self.geocodage_token
        )
        
        coord_mapping_data = []
        for idx, addr in enumerate(unique_addresses):
            coord = coordinates[idx] if idx < len(coordinates) else None
            if coord:
                coord_mapping_data.append({
                    "full_address": addr,
                    "latitude": coord[0],
                    "longitude": coord[1]
                })
            else:
                raise ValueError(f"Aucune coordonnée trouvée pour l'adresse {addr}")
        return coord_mapping_data
        
    
    
    def aggregate_sibil_meteo(
        self,
        csv_path: str,
        filters: Dict[str, Any] = None,
        sibil_columns: List[str] = None,
    ) -> DataFrame:
        """
        Agrège les données SIBIL avec les données météo.
        
        Args:
            csv_path: Chemin vers le fichier CSV SIBIL
            filters: Dictionnaire de filtres pour SIBIL {colonne: valeur}
            sibil_columns: Liste de colonnes SIBIL à conserver
        Returns:
            DataFrame Spark avec les données SIBIL et météo agrégées
        """
        if sibil_columns is None:
            sibil_columns = [
                "festival_nom", "lieu_nom", "lieu_adresse", "lieu_code_postal", 
                "lieu_ville", "lieu_departement_code", "declaration_date_representation"
            ]
        
        required_columns = ["lieu_adresse", "lieu_code_postal", "lieu_ville", "lieu_departement_code"]
        for col_name in required_columns:
            if col_name not in sibil_columns:
                sibil_columns.append(col_name)
        
        print("Extraction des données SIBIL...")
        df_sibil : DataFrame = self.extractor.extract_SIBIL_filtered(
            csv_path=csv_path,
            filters=filters,
            columns=sibil_columns
        )
        
        df_sibil = df_sibil.filter(
            (col("lieu_adresse").isNotNull()) & 
            (col("lieu_adresse") != "null") &
            (col("lieu_ville").isNotNull()) &
            (col("lieu_ville") != "null")
        )
        
        print("Ajout des coordonnées géographiques...")
        df_with_address = df_sibil.withColumn(
            "full_address",
            concat_ws(", ",
                trim(col("lieu_adresse")),
                trim(col("lieu_code_postal")),
                trim(col("lieu_ville"))
            )
        )
        
        unique_addresses_df = df_with_address.select("full_address").distinct()
        unique_addresses = [row.full_address for row in unique_addresses_df.collect() if row.full_address]

        coord_mapping_data = self._get_coordinates_for_df(unique_addresses)
        
        
        coord_mapping_df = self.extractor.spark.createDataFrame(coord_mapping_data)
        
        df_with_coords = df_with_address.join(
            coord_mapping_df,
            on="full_address",
            how="left"
        ).drop("full_address")

        df_with_coords = df_with_coords.filter(
            col("latitude").isNotNull() & col("longitude").isNotNull()
        )
        
        df_date_rename = df_with_coords.withColumnRenamed("declaration_date_representation", "day_representation")
        
        df_valid = df_date_rename.filter(
            col("latitude").isNotNull() & 
            col("longitude").isNotNull() &
            col("lieu_departement_code").isNotNull()
        )
        
        print(f"Traitement de {df_valid.count()} événements SIBIL avec données météo...")
        
        df_with_station_key = df_valid.withColumn(
            "station_key",
            concat_ws("_",
                col("latitude").cast("string"),
                col("longitude").cast("string"),
                col("lieu_departement_code").cast("string"),
                col("day_representation"),
            )
        )
        
        unique_combinations = df_with_station_key.select(
            "station_key", "latitude", "longitude", "lieu_departement_code", 
            "day_representation"
        ).distinct().collect()
        
        print(f"Traitement de {len(unique_combinations)} combinaisons uniques station/date...")
        
        meteo_data_cache = {}
        
        for combo in unique_combinations:
            lat = combo.latitude
            lon = combo.longitude
            dept = combo.lieu_departement_code
            day_representation = combo.day_representation
            key = combo.station_key
            print(f"  → Traitement de la combinaison {key}...")
            
            print(f"  → Recherche station pour ({lat}, {lon}) dans le département {dept}...")
            station_id = _find_station(self.meteo_token, float(lat), float(lon), int(dept), day_representation)
            
            if not station_id:
                print(f"  → Aucune station trouvée")
                continue
            
            print(f"  → Station: {station_id}, Date de représentation: {day_representation}")
            meteo_data = _get_weather_data(self.meteo_token, station_id, day_representation)
            meteo_df : DataFrame = self.extractor.spark.createDataFrame(meteo_data.to_dict(orient="records"))
            
            if len(meteo_df.columns) > 2:
                meteo_data_cache[key] = (station_id, meteo_df)
                print(f"  → {meteo_df.count()} jours de données météo récupérées")
            else:
                meteo_data_cache[key] = (station_id, None)
                print(f"  → Aucune donnée météo disponible")
            
            time.sleep(1)
        
        all_meteo_dfs = []
        
        for key, (station_id, meteo_df) in meteo_data_cache.items():
            if len(meteo_df.columns) > 2:
                meteo_with_key = meteo_df.withColumn("station_key", lit(key)) \
                                         .withColumn("station_id", lit(station_id))
                all_meteo_dfs.append(meteo_with_key)
        
        if all_meteo_dfs:
            from functools import reduce
            meteo_union = reduce(DataFrame.unionByName, all_meteo_dfs)
            
            final_df = df_with_station_key.join(
                meteo_union,
                on="station_key",
                how="left"
            ).drop("station_key")
        else:
            final_df = df_with_station_key.withColumn("station_id", lit(None).cast(IntegerType())) \
                                          .drop("station_key")
        
        return final_df


if __name__ == "__main__":
    load_dotenv()
    
    csv_path = "data/raw_sibil/Export_SIBIL_dataculture.csv"
    meteo_token = os.getenv("TOK")
    geocodage_token = os.getenv("GEOPF_TOKEN")
    
    if not meteo_token:
        raise ValueError("METEO_FRANCE_TOKEN doit être défini dans le fichier .env")
    
    with SIBILMeteoAggregator(meteo_token=meteo_token, geocodage_token=geocodage_token) as aggregator:
        df_result = aggregator.aggregate_sibil_meteo(
            csv_path=csv_path,
            filters={"festival_nom": "monatgne et nature "},
            sibil_columns=[
                "festival_nom", "lieu_nom", "lieu_adresse", 
                "lieu_code_postal", "lieu_ville", "lieu_departement_code",
                "declaration_date_representation"
            ],
        )
        
        print(f"Nombre de lignes dans le résultat: {df_result.count()}")
        df_result.show(20, truncate=False)
        
        df_result.write.mode("overwrite").parquet("data/processed/sibil_meteo_aggregated.parquet")
