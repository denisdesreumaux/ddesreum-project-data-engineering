import os
from datetime import datetime

import requests

def get_paris_realtime_bicycle_data():
    
    url = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/velib-disponibilite-en-temps-reel/exports/json"
    
    response = requests.request("GET", url)
    
    serialize_data(response.text, "paris_realtime_bicycle_data.json")

def get_nantes_realtime_bicycle_data():
    url = "https://data.nantesmetropole.fr/api/explore/v2.1/catalog/datasets/244400404_stations-velos-libre-service-nantes-metropole-disponibilites/exports/json"

    response = requests.request("GET", url)

    serialize_data(response.text, "nantes_realtime_bicycle_data.json")

def get_toulouse_realtime_bicycle_data():
    url = "https://data.toulouse-metropole.fr/api/explore/v2.1/catalog/datasets/api-velo-toulouse-temps-reel/exports/json"

    response = requests.request("GET", url)

    serialize_data(response.text, "toulouse_realtime_bicycle_data.json")

def get_strasbourg_realtime_bicycle_data():
    url = "https://data.strasbourg.eu/api/explore/v2.1/catalog/datasets/stations-velhop/exports/json"

    response = requests.request("GET", url)

    serialize_data(response.text, "strasbourg_realtime_bicycle_data.json")

def get_montpellier_realtime_bicycle_data():
    url = "https://portail-api-data.montpellier3m.fr/bikestation"
    header = {"accept": "application/json"}

    response = requests.get(url, headers=header)

    serialize_data(response.text, "montpellier_realtime_bicycle_data.json")

def get_commune_data():
    url = "https://geo.api.gouv.fr/communes"

    response = requests.request("GET", url)

    serialize_data(response.text, "commune_data.json")
    
def serialize_data(raw_json: str, file_name: str):

    today_date = datetime.now().strftime("%Y-%m-%d")
    
    if not os.path.exists(f"data/raw_data/{today_date}"):
        os.makedirs(f"data/raw_data/{today_date}")
    
    with open(f"data/raw_data/{today_date}/{file_name}", "w") as fd:
        fd.write(raw_json)
