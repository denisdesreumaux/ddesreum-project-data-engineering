import json
from datetime import datetime, date

import duckdb
import pandas as pd

today_date = datetime.now().strftime("%Y-%m-%d")
PARIS_CITY_CODE = 1
NANTES_CITY_CODE = 2

def create_consolidate_tables():
    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    with open("data/sql_statements/create_consolidate_tables.sql") as fd:
        statements = fd.read()
        for statement in statements.split(";"):
            print(statement)
            con.execute(statement)

def consolidate_station_paris_data():
    data = {}

    # Consolidation logic for Paris Bicycle data
    with open(f"data/raw_data/{today_date}/paris_realtime_bicycle_data.json") as fd:
        data = json.load(fd)
    
    paris_raw_data_df = pd.json_normalize(data)
    paris_raw_data_df["id"] = paris_raw_data_df["stationcode"].apply(lambda x: f"{PARIS_CITY_CODE}-{x}")
    paris_raw_data_df["address"] = None
    paris_raw_data_df["created_date"] = date.today()

    paris_station_data_df = paris_raw_data_df[[
        "id",
        "stationcode",
        "name",
        "nom_arrondissement_communes",
        "code_insee_commune",
        "address",
        "coordonnees_geo.lon",
        "coordonnees_geo.lat",
        "is_installed",
        "created_date",
        "capacity"
    ]]

    paris_station_data_df.rename(columns={
        "stationcode": "code",
        "name": "name",
        "coordonnees_geo.lon": "longitude",
        "coordonnees_geo.lat": "latitude",
        "is_installed": "status",
        "nom_arrondissement_communes": "city_name",
        "code_insee_commune": "city_code"
    }, inplace=True)

    return paris_station_data_df

def consolidate_station_nantes_data():
    data = {}
    
    # Consolidation logic for Paris Bicycle data
    with open(f"data/raw_data/{today_date}/nantes_realtime_bicycle_data.json") as fd:
        data = json.load(fd)
    
    nantes_raw_data_df = pd.json_normalize(data)
    nantes_raw_data_df["id"] = nantes_raw_data_df["number"].apply(lambda x: f"{NANTES_CITY_CODE}-{x}")
    nantes_raw_data_df["address"] = None
    nantes_raw_data_df["code_insee_commune"] = 0
    nantes_raw_data_df["created_date"] = date.today()

    nantes_station_data_df = nantes_raw_data_df[[
        "id",
        "number",
        "name",
        "contract_name",
        "address",
        "position.lon",
        "position.lat",
        "status",
        "created_date",
        "bike_stands",
        "code_insee_commune"
    ]]

    nantes_station_data_df.rename(columns={
        "number": "code",
        "position.lon": "longitude",
        "position.lat": "latitude",
        "contract_name": "city_name",
        "code_insee_commune": "city_code",
        "bike_stands": "capacity"
    }, inplace=True)

    return nantes_station_data_df

def consolidate_station_data():
    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)

    paris_data = consolidate_station_paris_data()
    nantes_data = consolidate_station_nantes_data()

    all_data = [paris_data, nantes_data]
    all_data = pd.concat(all_data)

    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION SELECT * FROM all_data;")


def consolidate_city_data():
    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    data = {}

    with open(f"data/raw_data/{today_date}/commune_data.json") as fd:
        data = json.load(fd)

    raw_data_df = pd.json_normalize(data)
    raw_data_df["nb_inhabitants"] = None
    raw_data_df["created_date"] = date.today()

    city_data_df = raw_data_df[[
        "code",
        "nom",
        "population"
    ]]

    city_data_df.rename(columns={
        "code": "id",
        "nom": "name",
        "population": "nb_inhabitants"
    }, inplace=True)

    city_data_df["created_date"] = date.today()

    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_CITY SELECT * FROM city_data_df;")

def former_consolidate_paris_data():

    data = {}

    with open(f"data/raw_data/{today_date}/paris_realtime_bicycle_data.json") as fd:
        data = json.load(fd)

    raw_data_df = pd.json_normalize(data)
    raw_data_df["nb_inhabitants"] = None

    paris_data_df = raw_data_df[[
        "code_insee_commune",
        "nom_arrondissement_communes",
        "nb_inhabitants"
    ]]
    paris_data_df.rename(columns={
        "code_insee_commune": "id",
        "nom_arrondissement_communes": "name"
    }, inplace=True)
    paris_data_df.drop_duplicates(inplace = True)

    paris_data_df["created_date"] = date.today()
    
    return paris_data_df

def former_consolidate_nantes_data():

    data = {}

    with open(f"data/raw_data/{today_date}/nantes_realtime_bicycle_data.json") as fd:
        data = json.load(fd)

    raw_data_df = pd.json_normalize(data)
    raw_data_df["nb_inhabitants"] = None
    raw_data_df["code_insee_commune"] = 2

    nantes_data_df = raw_data_df[[
        "code_insee_commune",
        "name",
        "nb_inhabitants"
    ]]
    nantes_data_df.rename(columns={
        "code": "id",
    }, inplace=True)
    nantes_data_df.drop_duplicates(inplace = True)

    nantes_data_df["created_date"] = date.today()
    
    return nantes_data_df


def former_consolidate_city_data():

    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    paris_data = former_consolidate_paris_data()
    nantes_data = former_consolidate_nantes_data()

    all_data = [paris_data, nantes_data]
    all_data = pd.concat(all_data)
    
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_CITY SELECT * FROM all_data;")

def consolidate_paris_station_statement_data():

    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    data = {}

    # Consolidate station statement data for Paris
    with open(f"data/raw_data/{today_date}/paris_realtime_bicycle_data.json") as fd:
        data = json.load(fd)

    paris_raw_data_df = pd.json_normalize(data)
    paris_raw_data_df["station_id"] = paris_raw_data_df["stationcode"].apply(lambda x: f"{PARIS_CITY_CODE}-{x}")
    paris_raw_data_df["created_date"] = date.today()
    paris_station_statement_data_df = paris_raw_data_df[[
        "station_id",
        "numdocksavailable",
        "numbikesavailable",
        "duedate",
        "created_date"
    ]]
    
    paris_station_statement_data_df.rename(columns={
        "numdocksavailable": "bicycle_docks_available",
        "numbikesavailable": "bicycle_available",
        "duedate": "last_statement_date",
    }, inplace=True)

    return paris_station_statement_data_df

def consolidate_nantes_station_statement_data():

    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    data = {}

    # Consolidate station statement data for Paris
    with open(f"data/raw_data/{today_date}/nantes_realtime_bicycle_data.json") as fd:
        data = json.load(fd)

    nantes_raw_data_df = pd.json_normalize(data)
    nantes_raw_data_df["station_id"] = nantes_raw_data_df["number"].apply(lambda x: f"{NANTES_CITY_CODE}-{x}")
    nantes_raw_data_df["created_date"] = date.today()
    nantes_station_statement_data_df = nantes_raw_data_df[[
        "station_id",
        "available_bike_stands",
        "available_bikes",
        "last_update",
        "created_date"
    ]]
    
    nantes_station_statement_data_df.rename(columns={
        "available_bike_stands": "bicycle_docks_available",
        "available_bikes": "bicycle_available",
        "last_update": "last_statement_date",
    }, inplace=True)

    return nantes_station_statement_data_df

def consolidate_station_statement_data():
    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)

    paris_data = consolidate_paris_station_statement_data()
    nantes_data = consolidate_nantes_station_statement_data()

    all_data = [paris_data, nantes_data]
    all_data = pd.concat(all_data)

    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION_STATEMENT SELECT * FROM all_data;") 
