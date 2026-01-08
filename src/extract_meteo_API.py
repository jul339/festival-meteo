from typing import Dict, Any, Optional
import requests

DEFAULT_PARAMETRE = "precipitation"

def get_liste_stations_quotidienne(
    id_departement: int, 
    token: str,
    parametre: Optional[str] = None
    ) -> Dict[Any, Any]:

    base_url = "https://public-api.meteofrance.fr/public/DPClim/v1"
    endpoint = "/liste-stations/quotidienne"
    url = f"{base_url}{endpoint}"
    params = {
        "id-departement": id_departement
    }
    
    if parametre:
        params["parametre"] = parametre
    
    headers = {
        "Authorization": f"Bearer {token}"
    }
    
    try:
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()  # Lève une exception pour les codes d'erreur HTTP
        return response.json()
    except requests.exceptions.HTTPError as e:
        print(f"Erreur HTTP lors de la récupération des données: {e}")
        if e.response is not None:
            print(f"Code de statut: {e.response.status_code}")
            try:
                error_detail = e.response.json()
                print(f"Détails de l'erreur: {error_detail}")
            except:
                print(f"Réponse: {e.response.text}")
        raise
    except requests.exceptions.RequestException as e:
        print(f"Erreur lors de la récupération des données: {e}")
        raise

def get_information_station(id_station: int, token: str) -> Dict[Any, Any]:
    base_url = "https://public-api.meteofrance.fr/public/DPClim/v1"
    endpoint = "/information-station"
    url = f"{base_url}{endpoint}"
    params = {
        "id-station": id_station
    }
    headers = {
        "Authorization": f"Bearer {token}"
    }
    response = requests.get(url, params=params, headers=headers)
    response.raise_for_status()
    return response.json()

def command_station_data_quotidienne(id_station: int, date_deb_periode: str, date_fin_periode: str, token: str) -> Dict[Any, Any]:
    base_url = "https://public-api.meteofrance.fr/public/DPClim/v1"
    endpoint = "/commande-station/quotidienne"
    url = f"{base_url}{endpoint}"
    date_deb_periode += "T00:00:00Z"
    date_fin_periode += "T00:00:00Z"
    params = {
        "id-station": id_station,
        "date-deb-periode": date_deb_periode,
        "date-fin-periode": date_fin_periode
    }
    headers = {
        "Authorization": f"Bearer {token}"
    }
    response = requests.get(url, params=params, headers=headers)
    response.raise_for_status()
    return response.json()

def get_csv_from_command_id(command_id: int, token: str) -> Dict[Any, Any]:
    base_url = "https://public-api.meteofrance.fr/public/DPClim/v1"
    endpoint = "/commande/fichier"
    url = f"{base_url}{endpoint}"
    params = {
        "id-cmde": command_id
    }
    headers = {
        "Authorization": f"Bearer {token}"
    }
    response = requests.get(url, params=params, headers=headers)
    response.raise_for_status()
    return response.text

