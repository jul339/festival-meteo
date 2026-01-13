import requests
import csv
import io
from typing import Optional, Tuple, Dict, Any, List


def get_coordinates_from_addresses_batch(
    addresses: List[str], token: Optional[str] = None, column_name: str = "address"
) -> List[Optional[Tuple[float, float]]]:
    """
    Récupère les coordonnées (latitude, longitude) pour plusieurs adresses
    en utilisant l'API de géocodage batch de la Géoplateforme.

    Args:
        addresses: Liste d'adresses à géocoder
        token: Token d'authentification (peut être None si l'API ne nécessite pas d'auth)
        column_name: Nom de la colonne dans le CSV (par défaut "address")

    Returns:
        Liste de tuples (latitude, longitude) ou None pour chaque adresse
    """
    if not addresses:
        return []

    base_url = "https://data.geopf.fr/geocodage"
    endpoint = "/search/csv"
    url = f"{base_url}{endpoint}"

    # Créer un CSV en mémoire avec les adresses
    csv_buffer = io.StringIO()
    writer = csv.writer(csv_buffer)

    # Écrire l'en-tête
    writer.writerow([column_name])

    # Écrire les adresses
    for address in addresses:
        writer.writerow([address])

    csv_content = csv_buffer.getvalue()
    csv_buffer.close()

    # Préparer les données pour la requête multipart/form-data
    files = {"data": (f"{column_name}.csv", csv_content.encode("utf-8"), "text/csv")}

    # Paramètres de la requête
    data = {
        "columns": [column_name],  # Colonne à utiliser pour le géocodage
        "indexes": ["address"],  # Index à utiliser (address, poi, parcel)
        "result_columns": ["latitude", "longitude", "result_score", "result_status"],
    }

    # Headers avec authentification si nécessaire
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    try:
        response = requests.post(url, files=files, data=data, headers=headers)
        response.raise_for_status()

        # Parser le CSV de réponse
        csv_response = io.StringIO(response.text)
        reader = csv.DictReader(csv_response)

        results = []
        for row in reader:
            # Vérifier le statut du résultat
            status = row.get("result_status", "")
            if status == "ok":
                try:
                    latitude = float(row.get("latitude", 0))
                    longitude = float(row.get("longitude", 0))
                    if latitude != 0 and longitude != 0:
                        results.append((latitude, longitude))
                    else:
                        results.append(None)
                except (ValueError, TypeError):
                    results.append(None)
            else:
                # not-found, skipped, ou error
                results.append(None)

        csv_response.close()
        return results

    except requests.exceptions.HTTPError as e:
        print(f"Erreur HTTP lors du géocodage batch: {e}")
        if e.response is not None:
            print(f"Code de statut: {e.response.status_code}")
            try:
                error_detail = e.response.json()
                print(f"Détails de l'erreur: {error_detail}")
            except:
                print(f"Réponse: {e.response.text}")
        # Retourner une liste de None en cas d'erreur
        return [None] * len(addresses)
    except requests.exceptions.RequestException as e:
        print(f"Erreur lors du géocodage batch: {e}")
        return [None] * len(addresses)
    except (KeyError, IndexError, ValueError) as e:
        print(f"Erreur lors de l'extraction des coordonnées: {e}")
        return [None] * len(addresses)


def get_coordinates_for_df(
    unique_addresses: List[str], token: Optional[str] = None
) -> list[str]:
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

    coordinates = get_coordinates_from_addresses_batch(unique_addresses, token=token)

    coord_mapping_data = []
    for idx, addr in enumerate(unique_addresses):
        coord = coordinates[idx] if idx < len(coordinates) else None
        if coord:
            coord_mapping_data.append(
                {"full_address": addr, "latitude": coord[0], "longitude": coord[1]}
            )
        else:
            raise ValueError(f"Aucune coordonnée trouvée pour l'adresse {addr}")
    return coord_mapping_data
