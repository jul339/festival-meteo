from typing import Dict, Any, Optional
import pandas as pd

import requests
from datetime import datetime
import os
import time
from io import StringIO
import logging as log

DEFAULT_PARAMETRE = "precipitation"
log.basicConfig(level=log.INFO)


def get_liste_stations_quotidienne(
    id_departement: int, token: str, parametre: Optional[str] = None
) -> Dict[Any, Any]:

    base_url = "https://public-api.meteofrance.fr/public/DPClim/v1"
    endpoint = "/liste-stations/quotidienne"
    url = f"{base_url}{endpoint}"
    params = {"id-departement": id_departement}

    if parametre:
        params["parametre"] = parametre

    headers = {"Authorization": f"Bearer {token}"}

    try:
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()  # Lève une exception pour les codes d'erreur HTTP
        return response.json()
    except requests.exceptions.HTTPError as e:
        log.error(f"Erreur HTTP lors de la récupération des données: {e}")
        if e.response is not None:
            log.error(f"Code de statut: {e.response.status_code}")
            try:
                error_detail = e.response.json()
                log.error(f"Détails de l'erreur: {error_detail}")
            except:
                log.error(f"Réponse: {e.response.text}")
        raise
    except requests.exceptions.RequestException as e:
        log.error(f"Erreur lors de la récupération des données: {e}")
        raise


def get_information_station(id_station: int, token: str) -> Dict[Any, Any]:
    base_url = "https://public-api.meteofrance.fr/public/DPClim/v1"
    endpoint = "/information-station"
    url = f"{base_url}{endpoint}"
    params = {"id-station": id_station}
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, params=params, headers=headers)
    response.raise_for_status()
    return response.json()


def command_station_data_quotidienne(
    id_station: int, day_representation: str, token: str
) -> Dict[Any, Any]:
    base_url = "https://public-api.meteofrance.fr/public/DPClim/v1"
    endpoint = "/commande-station/quotidienne"
    url = f"{base_url}{endpoint}"
    day_representation = datetime.strptime(day_representation, "%d-%m-%Y").strftime(
        "%Y-%m-%d"
    )
    date_deb_periode = day_representation + "T00:00:00Z"
    date_fin_periode = day_representation + "T23:59:59Z"
    params = {
        "id-station": id_station,
        "date-deb-periode": date_deb_periode,
        "date-fin-periode": date_fin_periode,
    }
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, params=params, headers=headers)
    response.raise_for_status()
    return response.json()


def get_csv_from_command_id(command_id: int, token: str) -> Dict[Any, Any]:
    base_url = "https://public-api.meteofrance.fr/public/DPClim/v1"
    endpoint = "/commande/fichier"
    url = f"{base_url}{endpoint}"
    params = {"id-cmde": command_id}
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, params=params, headers=headers)
    response.raise_for_status()
    return response.text


def _find_station(
    token: str,
    latitude: float,
    longitude: float,
    departement_code: int,
    day_representation: str,
) -> Optional[int]:
    """
    Trouve la station météo la plus proche pour des coordonnées données.

    Args:
        latitude: Latitude
        longitude: Longitude
        departement_code: Code du département
        day_representation: Date de représentation au format YYYY-MM-DD

    Returns:
        ID de la station la plus proche ou None
    """
    try:
        stations_data = get_liste_stations_quotidienne(
            id_departement=departement_code, token=token
        )

        if not stations_data:
            return None
        if "data" in stations_data:
            stations = stations_data["data"]
            if not stations:
                return None
        else:
            stations = stations_data

        min_distance = float("inf")
        nearest_station_id = None
        day_dt = datetime.strptime(day_representation, "%d-%m-%Y")

        for station in stations:
            if station.get("posteOuvert") == False:
                continue
            try:
                station_id = station.get("id")
                if not station_id:
                    continue

                station_lat = station.get("lat")
                station_lon = station.get("lon")

                if station_lat is not None and station_lon is not None:
                    distance = (
                        (latitude - station_lat) ** 2 + (longitude - station_lon) ** 2
                    ) ** 0.5

                    if distance < min_distance:
                        info_station = get_information_station(station_id, token)[0]
                        if info_station:
                            start_date_meteo_dt = datetime.strptime(
                                info_station.get("dateDebut"), "%Y-%m-%d %H:%M:%S"
                            )
                            end_date_meteo_dt = info_station.get("dateFin")
                            if end_date_meteo_dt == "" or end_date_meteo_dt == None:
                                end_date_meteo_dt = datetime.now()
                            else:
                                end_date_meteo_dt = datetime.strptime(
                                    end_date_meteo_dt, "%Y-%m-%d %H:%M:%S"
                                )
                            if (
                                start_date_meteo_dt <= day_dt
                                and end_date_meteo_dt >= day_dt
                            ):
                                min_distance = distance
                                nearest_station_id = station_id
                    else:
                        continue

            except Exception as e:
                log.error(f"Erreur lors du traitement de la station {station_id}: {e}")
                continue

        return nearest_station_id

    except Exception as e:
        log.error(f"Erreur lors de la recherche de la station la plus proche: {e}")
        return None


def _get_weather_data(
    token: str, station_id: int, day_representation: str
) -> Optional[pd.DataFrame]:
    """
    Récupère les données météo pour une station et une période données.

    Args:
        station_id: ID de la station météo
        day_representation: Date de représentation au format YYYY-MM-DD

    Returns:
        DataFrame Spark avec les données météo ou None
    """
    try:
        command_response = command_station_data_quotidienne(
            id_station=station_id, day_representation=day_representation, token=token
        )

        if not command_response:
            log.error(f"Erreur: réponse vide")
            return None

        try:
            command_id = command_response.get(
                "elaboreProduitAvecDemandeResponse", {}
            ).get("return")
            if not command_id:
                log.error(
                    f"Erreur: pas d'ID de commande dans la réponse, structure de la réponse: {command_response}"
                )
                return None
        except (KeyError, AttributeError) as e:
            log.error(f"Erreur lors de l'extraction de l'ID de commande: {e}")
            log.error(f"Structure de la réponse: {command_response}")
            return None

        max_attempts = 2
        for attempt in range(max_attempts):
            try:
                csv_data = get_csv_from_command_id(command_id, token)
            except Exception as e:
                log.error(f"Erreur lors de la récupération des données météo: {e}")
                csv_data = ""

            if csv_data and len(csv_data.strip()) > 0:
                try:
                    df_meteo = pd.read_csv(
                        StringIO(csv_data),
                        sep=";",
                        decimal=",",
                        na_values=["", "NA", "NaN", "null", "None"],
                        keep_default_na=True,
                    )
                except Exception as e:
                    log.error(f"Erreur lors de la lecture du fichier CSV: {e}")
                    df_meteo = None

                if df_meteo is not None and not df_meteo.empty:
                    df_meteo = df_meteo.dropna(axis=1, how="all")
                    q_cols = [c for c in df_meteo.columns if str(c).startswith("Q")]
                    if q_cols:
                        df_meteo = df_meteo.drop(columns=q_cols, errors="ignore")
                    return df_meteo

            time.sleep(min(2**attempt, 20))

        log.error(
            f"Timeout: la commande {command_id} n'est pas prête après {max_attempts} tentatives"
        )
        return None

    except Exception as e:
        log.error(f"Erreur lors de la récupération des données météo: {e}")
        return None
