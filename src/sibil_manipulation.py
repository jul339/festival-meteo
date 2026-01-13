from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, isnan, isnull
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DateType,
    LongType,
    BooleanType,
)
import os

from typing import Optional
from dotenv import load_dotenv

load_dotenv()


class SIBILExtractor:
    """
    Classe pour extraire et traiter des fichiers CSV SIBIL avec PySpark.
    """

    def __init__(self, spark: Optional[SparkSession] = None):
        """
        Initialise l'extracteur SIBIL.

        Args:
            spark: Session Spark optionnelle. Si None, en crée une nouvelle.
        """
        if spark is None:
            self.spark = (
                SparkSession.builder.appName("SIBIL_Extraction")
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .getOrCreate()
            )
            self.spark_created_here = True
        else:
            self.spark = spark
            self.spark_created_here = False

    def __enter__(self):
        """Support pour le context manager (with statement)."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Arrête Spark à la sortie du context manager."""
        if self.spark_created_here:
            self.spark.stop()

    def stop(self):
        """Arrête manuellement la session Spark."""
        if self.spark_created_here:
            self.spark.stop()
            self.spark_created_here = False

    def extract_SIBIL_filtered(
        self, csv_path: str, filters: dict = None, columns: list = None
    ) -> DataFrame:
        """
        Extrait un CSV avec filtres et sélection de colonnes.

        Args:
            csv_path: Chemin vers le fichier CSV
            filters: Dictionnaire de filtres {colonne: valeur}
            columns: Liste de colonnes à extraire

        Returns:
            DataFrame Spark filtré et sélectionné
        """
        schema = StructType(
            [
                StructField("declaration_creation", StringType(), True),
                StructField("declaration_modification", StringType(), True),
                StructField("declarant_ID_sibil", StringType(), True),
                StructField("declarant_nom", StringType(), True),
                StructField("declarant_nom_usuel", StringType(), True),
                StructField("declarant_statut", StringType(), True),
                StructField("demandeur_ID_sibil", IntegerType(), True),
                StructField("demandeur_role", StringType(), True),
                StructField("declarant_siret", StringType(), True),
                StructField("declarant_code_postal", StringType(), True),
                StructField("declarant_ville", StringType(), True),
                StructField("declarant_departement", StringType(), True),
                StructField("declarant_region", StringType(), True),
                StructField("declarant_numero_dossier", StringType(), True),
                StructField("declarant_numero_licence", StringType(), True),
                StructField("declaration_ID_sibil", IntegerType(), True),
                StructField("declaration_ID_externe", StringType(), True),
                StructField("declaration_date_representation", StringType(), True),
                StructField("declaration_statut", StringType(), True),
                StructField("declaration_cause_erreur", StringType(), True),
                StructField("declaration_gestionnaires_multiples", BooleanType(), True),
                StructField("festival_ID_parent_sibil", IntegerType(), True),
                StructField("festival_nom_parent", StringType(), True),
                StructField("festival_ID_sibil", IntegerType(), True),
                StructField("festival_nom", StringType(), True),
                StructField("festival_ville", StringType(), True),
                StructField("festival_code_postal", IntegerType(), True),
                StructField("festival_date_creation", StringType(), True),
                StructField("festival_statut", StringType(), True),
                StructField("festival_contributeur_ID_sibil", IntegerType(), True),
                StructField("festival_validateur_ID_sibil", IntegerType(), True),
                StructField("festival_cause_erreur_adresse", StringType(), True),
                StructField("lieu_ID_parent_sibil", IntegerType(), True),
                StructField("lieu_nom_parent", StringType(), True),
                StructField("lieu_ID_sibil", IntegerType(), True),
                StructField("lieu_nom", StringType(), True),
                StructField("lieu_nom_usuel", StringType(), True),
                StructField("lieu_adresse", StringType(), True),
                StructField("lieu_code_postal", StringType(), True),
                StructField("lieu_ville", StringType(), True),
                StructField("lieu_siret", LongType(), True),
                StructField("lieu_departement_code", IntegerType(), True),
                StructField("lieu_departement_nom", StringType(), True),
                StructField("lieu_date_creation", StringType(), True),
                StructField("lieu_statut", StringType(), True),
                StructField("lieu_contributeur_ID_sibil", IntegerType(), True),
                StructField("lieu_validateur_ID_sibil", IntegerType(), True),
                StructField("lieu_cause_erreur_adresse", StringType(), True),
            ]
        )
        df = (
            self.spark.read.option("header", "true")
            .option("schema", schema)
            .option("delimiter", ";")
            .option("quote", '"')
            .option("encoding", "UTF-8")
            .csv(csv_path)
        )

        # Appliquer les filtres
        if filters:
            for column, value in filters.items():
                df = df.filter(col(column) == value)

        # Sélectionner les colonnes
        if columns:
            df = df.select(*columns)

        df.dropna

        return df


def write_csv_unique_festivals_names(df: DataFrame) -> list[str]:
    """
    Écrit un fichier CSV avec les noms de festivals uniques d'un DataFrame SIBIL.
    """
    df.select("festival_nom").distinct().write.csv(
        "data/unique_festivals_names.csv", header=True
    )


# Exemple d'utilisation
if __name__ == "__main__":
    csv_path = "data/raw_sibil/Export_SIBIL_dataculture.csv"

    # Utiliser la classe avec context manager (recommandé)
    with SIBILExtractor() as extractor:
        # Extraction simple avec la session Spark de la classe

        # Extraction avec filtres
        df_filtered = extractor.extract_SIBIL_filtered(
            csv_path,
            filters={"festival_nom": "GAROROCK Experience"},
            columns=[
                "festival_date_creation",
                "lieu_date_creation",
                "declaration_date_representation",
                "festival_nom",
                "lieu_nom",
                "lieu_adresse",
                "lieu_ville",
                "festival_nom_parent",
                "declarant_nom",
                "declarant_nom_usuel",
                "lieu_departement_code",
            ],
        )
        # write_csv_unique_festivals_names(df_filtered)
        df_filtered.write.csv("data/sibil_filtered.csv", header=True)
        print(df_filtered[0]["lieu_adresse"])
        df_filtered.show(10)
