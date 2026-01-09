import os
import pandas as pd
import requests
from typing import Optional, Dict, Any
from dotenv import load_dotenv
load_dotenv()
import pprint


from extract_meteo_API import get_liste_stations_quotidienne, get_information_station, command_station_data_quotidienne, get_csv_from_command_id
from get_coord_API import get_coordinates_from_addresses_batch
TOKEN = os.getenv("TOK")
if not TOKEN:
    raise ValueError("Token API requis. Fournissez-le en paramètre 'token' ou définissez "
        "la variable d'environnement TOK"
    )


def main():
    # station = get_information_station(31006001, TOKEN)
    # pprint.pprint(station)
    # print(TOKEN)

    # departement = 31
    # stations = get_liste_stations_quotidienne(departement, TOKEN, )
    # print(f"Nombre de stations trouvées: {len(stations) if isinstance(stations, list) else 'N/A'}")
    # df = pd.DataFrame(stations)
    # df.to_csv("data/stations_quotidienne_31.csv", index=False)

    
    # df = pd.read_csv("data/stations_quotidienne_31.csv")
    # id = df[df["posteOuvert"]].iloc[0]["id"]
    station = command_station_data_quotidienne(31424001, "2025-11-28", "2025-12-05", TOKEN)
    print(len(station))
    pprint.pprint(station, depth=4)

    # csv = get_csv_from_command_id(2026000865041, TOKEN)
    # # df = pd.read_csv(csv)
    # pprint.pprint(csv)
    # with open("data/raw_meteo/test.csv", "w") as f:
    #     f.write(csv)


    # coordinates = get_coordinates_from_addresses_batch(["34 rue Gabriel Péri, 94 200"], TOKEN)
    # pprint.pprint(coordinates)

if __name__ == "__main__":
    main()
