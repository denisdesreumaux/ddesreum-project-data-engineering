import json
from datetime import datetime, date

import duckdb
import pandas as pd

today_date = datetime.now().strftime("%Y-%m-%d")

# Const for each city used
PARIS_CITY_CODE = 1
NANTES_CITY_CODE = 2
TOULOUSE_CITY_CODE = 3
STRASBOURG_CITY_CODE = 4
MONTPELLIER_CITY_CODE = 5

def create_consolidate_tables():
    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    with open("data/sql_statements/create_consolidate_tables.sql") as fd:
        statements = fd.read()
        for statement in statements.split(";"):
            print(statement)
            con.execute(statement)

def consolidate_station_data():
    """
    Call each function for each city that will provide data for the CONSOLIDATE_STATION table
    """

    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)

    # Retrieve all data frames
    paris_data = consolidate_station_paris_data()
    nantes_data = consolidate_station_nantes_data()
    toulouse_data = consolidate_station_toulouse_data()
    strasbourg_data = consolidate_station_strasbourg_data()
    montpellier_data = consolidate_station_montpellier_data()

    # Merge the data frames
    all_data = [paris_data, nantes_data, toulouse_data, strasbourg_data, montpellier_data]
    all_data = pd.concat(all_data)

    # Push the merged data frame into the CONSOLIDATE_STATION table
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION SELECT * FROM all_data;")

def consolidate_station_statement_data():
    """
    Call each function for each city that will provide data for the CONSOLIDATE_STATION_STATEMENT table
    """

    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)

    # Retrieve all data frames
    paris_data = consolidate_paris_station_statement_data()
    nantes_data = consolidate_nantes_station_statement_data()
    toulouse_data = consolidate_toulouse_station_statement_data()
    strasbourg_data = consolidate_strasbourg_station_statement_data()
    montpellier_data = consolidate_montpellier_station_statement_data()

    # Merge the data frames
    all_data = [paris_data, nantes_data, toulouse_data, strasbourg_data, montpellier_data]
    all_data = pd.concat(all_data)

    # Push the merged data frame into the CONSOLIDATE_STATION_STATEMENT table
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION_STATEMENT SELECT * FROM all_data;") 

def consolidate_city_data():
    """
    Retrieve city data and insert it inside the CONSOLIDATE_CITY table
    """

    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    data = {}

    # Get data from the JSON file
    with open(f"data/raw_data/{today_date}/commune_data.json") as fd:
        data = json.load(fd)


    raw_data_df = pd.json_normalize(data)

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

""" The functions below are used by consolidate_station_data """

def consolidate_station_paris_data():
    """
    Retrieve data from the JSON file for Paris and processes it to match the 
    format and the constraints of the CONSOLIDATE_STATION 

    Returns : paris_station_data_df, a pandas data frame containing the consolidated data
    """

    # Get data from the JSON file
    data = {}

    with open(f"data/raw_data/{today_date}/paris_realtime_bicycle_data.json") as fd:
        data = json.load(fd)
    
    # Format the data
    paris_raw_data_df = pd.json_normalize(data)
    paris_raw_data_df = consolidate_format_df(paris_raw_data_df, "stationcode", PARIS_CITY_CODE)

    # Standardization of the status between all APIs
    paris_raw_data_df["is_installed"] = paris_raw_data_df["is_installed"].apply(lambda x: "OPEN" if x == "OUI" else "CLOSED")

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

    # Rename the columns of the data frame
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
    """
    Retrieve data from the JSON file for Nantes and processes it to match the 
    format and the constraints of the CONSOLIDATE_STATION 

    Returns : nantes_station_data_df, a pandas data frame containing the consolidated data
    """
    
    # Get data from the JSON file
    data = {}

    with open(f"data/raw_data/{today_date}/nantes_realtime_bicycle_data.json") as fd:
        data = json.load(fd)
    
    # Format the data
    nantes_raw_data_df = pd.json_normalize(data)
    nantes_raw_data_df = consolidate_format_df(nantes_raw_data_df, "number", NANTES_CITY_CODE)
    nantes_raw_data_df["code_insee_commune"] = get_insee_code("Nantes")

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

    # Rename the columns of the data frame
    nantes_station_data_df.rename(columns={
        "number": "code",
        "position.lon": "longitude",
        "position.lat": "latitude",
        "contract_name": "city_name",
        "code_insee_commune": "city_code",
        "bike_stands": "capacity"
    }, inplace=True)

    return nantes_station_data_df

def consolidate_station_toulouse_data():
    """
    Retrieve data from the JSON file for Toulouse and processes it to match the 
    format and the constraints of the CONSOLIDATE_STATION 

    Returns : toulouse_station_data_df, a pandas data frame containing the consolidated data
    """
    
    # Get data from the JSON file
    data = {}
    
    with open(f"data/raw_data/{today_date}/toulouse_realtime_bicycle_data.json") as fd:
        data = json.load(fd)
    
    # Format the data
    toulouse_raw_data_df = pd.json_normalize(data)
    toulouse_raw_data_df = consolidate_format_df(toulouse_raw_data_df, "number", TOULOUSE_CITY_CODE)
    toulouse_raw_data_df["code_insee_commune"] = get_insee_code("Toulouse")

    toulouse_station_data_df = toulouse_raw_data_df[[
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

    # Rename the columns of the data frame
    toulouse_station_data_df.rename(columns={
        "number": "code",
        "position.lon": "longitude",
        "position.lat": "latitude",
        "contract_name": "city_name",
        "code_insee_commune": "city_code",
        "bike_stands": "capacity"
    }, inplace=True)

    return toulouse_station_data_df

def consolidate_station_strasbourg_data():
    """
    Retrieve data from the JSON file for Strasbourg and processes it to match the 
    format and the constraints of the CONSOLIDATE_STATION 

    Returns : strasbourg_station_data_df, a pandas data frame containing the consolidated data
    """
    
    # Get data from the JSON file
    data = {}
    
    with open(f"data/raw_data/{today_date}/strasbourg_realtime_bicycle_data.json") as fd:
        data = json.load(fd)
    
    # Format the data
    strasbourg_raw_data_df = pd.json_normalize(data)
    strasbourg_raw_data_df["number"] = strasbourg_raw_data_df["id"]
    strasbourg_raw_data_df = consolidate_format_df(strasbourg_raw_data_df, "id", STRASBOURG_CITY_CODE)
    strasbourg_raw_data_df["code_insee_commune"] = get_insee_code("Strasbourg")

    # Standardization of the status between all APIs
    strasbourg_raw_data_df["is_installed"] = strasbourg_raw_data_df["is_installed"].apply(lambda x: "OPEN" if x == "1" else "CLOSED")

    # There's no information about the name of the city inside the dataset. Therefore, city_name is forced
    strasbourg_raw_data_df["city_name"] = "strasbourg"

    strasbourg_station_data_df = strasbourg_raw_data_df[[
        "id",
        "number",
        "na",
        "city_name",
        "address",
        "lon",
        "lat",
        "is_installed",
        "created_date",
        "to",
        "code_insee_commune"
    ]]

    # Rename the columns of the data frame
    strasbourg_station_data_df.rename(columns={
        "number": "code",
        "is_installed": "status",
        "lon": "longitude",
        "lat": "latitude",
        "code_insee_commune": "city_code",
        "na": "name",
        "to": "capacity"
    }, inplace=True)

    return strasbourg_station_data_df

def consolidate_station_montpellier_data():
    """
    Retrieve data from the JSON file for Montpellier and processes it to match the 
    format and the constraints of the CONSOLIDATE_STATION 

    Returns : montpellier_station_data_df, a pandas data frame containing the consolidated data
    """
    
    # Get data from the JSON file
    data = {}
    
    with open(f"data/raw_data/{today_date}/montpellier_realtime_bicycle_data.json") as fd:
        data = json.load(fd)
    
    montpellier_raw_data_df = pd.json_normalize(data)
    montpellier_raw_data_df["number"] = montpellier_raw_data_df["id"].str[-3:]
    montpellier_raw_data_df = consolidate_format_df(montpellier_raw_data_df, "number", MONTPELLIER_CITY_CODE)
    montpellier_raw_data_df["code_insee_commune"] = get_insee_code("Montpellier")

    # Standardization of the status between all APIs
    montpellier_raw_data_df["status.value"] = montpellier_raw_data_df["status.value"].apply(lambda x: "OPEN" if x == "working" else "CLOSED")

    # The coordinates are formatted inside a list. Unwind it to get only the longitude and the latitude
    montpellier_raw_data_df["longitude"] = montpellier_raw_data_df["location.value.coordinates"].apply(lambda x: x[0])
    montpellier_raw_data_df["latitude"] = montpellier_raw_data_df["location.value.coordinates"].apply(lambda x: x[1])

    montpellier_station_data_df = montpellier_raw_data_df[[
        "id",
        "number",
        "address.value.streetAddress",
        "address.value.addressLocality",
        "address",
        "longitude",
        "latitude",
        "status.value",
        "created_date",
        "totalSlotNumber.value",
        "code_insee_commune"
    ]]

    # Rename the columns of the data frame
    montpellier_station_data_df.rename(columns={
        "number": "code",
        "status.value": "status",
        "address.value.addressLocality": "city_name",
        "code_insee_commune": "city_code",
        "address.value.streetAddress": "name",
        "totalSlotNumber.value": "capacity"
    }, inplace=True)

    return montpellier_station_data_df

""" The functions below are used by consolidate_station_statement_data """

def consolidate_paris_station_statement_data():
    """
    Retrieve data from the JSON file for Paris and processes it to match the 
    format and the constraints of the CONSOLIDATE_STATEMENT_STATION 

    Returns : paris_station_data_df, a pandas data frame containing the consolidated data
    """
    
    # Get data from the JSON file
    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    data = {}

    with open(f"data/raw_data/{today_date}/paris_realtime_bicycle_data.json") as fd:
        data = json.load(fd)

    # Format the data
    paris_raw_data_df = pd.json_normalize(data)
    paris_raw_data_df = consolidate_statement_city_data(paris_raw_data_df, "stationcode", PARIS_CITY_CODE)

    paris_station_statement_data_df = paris_raw_data_df[[
        "station_id",
        "numdocksavailable",
        "numbikesavailable",
        "duedate",
        "created_date"
    ]]
    
    # Rename the columns of the data frame
    paris_station_statement_data_df.rename(columns={
        "numdocksavailable": "bicycle_docks_available",
        "numbikesavailable": "bicycle_available",
        "duedate": "last_statement_date",
    }, inplace=True)

    return paris_station_statement_data_df

def consolidate_nantes_station_statement_data():
    """
    Retrieve data from the JSON file for Nantes and processes it to match the 
    format and the constraints of the CONSOLIDATE_STATEMENT_STATION 

    Returns : nantes_station_data_df, a pandas data frame containing the consolidated data
    """

    # Get data from the JSON file
    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    data = {}

    with open(f"data/raw_data/{today_date}/nantes_realtime_bicycle_data.json") as fd:
        data = json.load(fd)

    # Format the data
    nantes_raw_data_df = pd.json_normalize(data)
    nantes_raw_data_df = consolidate_statement_city_data(nantes_raw_data_df, "number", NANTES_CITY_CODE)
    
    nantes_station_statement_data_df = nantes_raw_data_df[[
        "station_id",
        "available_bike_stands",
        "available_bikes",
        "last_update",
        "created_date"
    ]]
    
    # Rename columns of the data frame
    nantes_station_statement_data_df.rename(columns={
        "available_bike_stands": "bicycle_docks_available",
        "available_bikes": "bicycle_available",
        "last_update": "last_statement_date",
    }, inplace=True)

    return nantes_station_statement_data_df

def consolidate_toulouse_station_statement_data():
    """
    Retrieve data from the JSON file for Toulouse and processes it to match the 
    format and the constraints of the CONSOLIDATE_STATEMENT_STATION 

    Returns : toulouse_station_data_df, a pandas data frame containing the consolidated data
    """

    # Get data from the JSON file
    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    data = {}

    with open(f"data/raw_data/{today_date}/toulouse_realtime_bicycle_data.json") as fd:
        data = json.load(fd)

    # Format the data
    toulouse_raw_data_df = pd.json_normalize(data)
    toulouse_raw_data_df = consolidate_statement_city_data(toulouse_raw_data_df, "number", TOULOUSE_CITY_CODE)

    toulouse_station_statement_data_df = toulouse_raw_data_df[[
        "station_id",
        "available_bike_stands",
        "available_bikes",
        "last_update",
        "created_date"
    ]]
    
    # Rename columns of the data frame
    toulouse_station_statement_data_df.rename(columns={
        "available_bike_stands": "bicycle_docks_available",
        "available_bikes": "bicycle_available",
        "last_update": "last_statement_date",
    }, inplace=True)

    return toulouse_station_statement_data_df

def consolidate_strasbourg_station_statement_data():
    """
    Retrieve data from the JSON file for Strasbourg and processes it to match the 
    format and the constraints of the CONSOLIDATE_STATEMENT_STATION 

    Returns : strasbourg_station_data_df, a pandas data frame containing the consolidated data
    """

    # Get data from the JSON file
    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    data = {}

    with open(f"data/raw_data/{today_date}/strasbourg_realtime_bicycle_data.json") as fd:
        data = json.load(fd)

    # Format the data
    strasbourg_raw_data_df = pd.json_normalize(data)
    strasbourg_raw_data_df = consolidate_statement_city_data(strasbourg_raw_data_df, "id", STRASBOURG_CITY_CODE)

    # The date is a timestamp, need to convert it to a datetime
    strasbourg_raw_data_df["last_reported"] = strasbourg_raw_data_df["last_reported"].apply(lambda x: datetime.fromtimestamp(int(x)))

    strasbourg_station_statement_data_df = strasbourg_raw_data_df[[
        "station_id",
        "av",
        "to",
        "last_reported",
        "created_date"
    ]]
    
    # Rename the columns of the data frame
    strasbourg_station_statement_data_df.rename(columns={
        "av": "bicycle_docks_available",
        "to": "bicycle_available",
        "last_reported": "last_statement_date",
    }, inplace=True)

    return strasbourg_station_statement_data_df

def consolidate_montpellier_station_statement_data():
    """
    Retrieve data from the JSON file for Montpellier and processes it to match the 
    format and the constraints of the CONSOLIDATE_STATEMENT_STATION 

    Returns : montpellier_station_data_df, a pandas data frame containing the consolidated data
    """

    # Get data from the JSON file
    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    data = {}

    with open(f"data/raw_data/{today_date}/montpellier_realtime_bicycle_data.json") as fd:
        data = json.load(fd)

    # Format the data
    montpellier_raw_data_df = pd.json_normalize(data)
    montpellier_raw_data_df["id"] = montpellier_raw_data_df["id"].str[-3:]

    montpellier_raw_data_df = consolidate_statement_city_data(montpellier_raw_data_df, "id", MONTPELLIER_CITY_CODE)

    montpellier_station_statement_data_df = montpellier_raw_data_df[[
        "station_id",
        "availableBikeNumber.value",
        "totalSlotNumber.value",
        "availableBikeNumber.metadata.timestamp.value",
        "created_date"
    ]]
    
    # Rename the columns of the date frame
    montpellier_station_statement_data_df.rename(columns={
        "availableBikeNumber.value": "bicycle_docks_available",
        "totalSlotNumber.value": "bicycle_available",
        "availableBikeNumber.metadata.timestamp.value": "last_statement_date",
    }, inplace=True)

    return montpellier_station_statement_data_df
    

def consolidate_format_df(df, column_source, code):
    """
    Format a data frame that will be used for the table CONSOLIDATE_STATION

    Params : 
        - df : pandas data frame, containing the data
        - column_source : string, name of the column where the station code is
        - code : const, city code used 

    Returns : df, pandas data frame modified
    """
    
    df["id"] = df[column_source].apply(lambda x: f"{code}-{x}")
    df["address"] = None
    df["created_date"] = date.today()

    return df


def consolidate_statement_city_data(df, column_source, code):
    """
    Format a data frame that will be used for the table CONSOLIDATE_STATION_STATEMENT

    Params : 
        - df : pandas data frame, containing the data
        - column_source : string, name of the column where the station code is
        - code : const, city code used 

    Returns : df, pandas data frame modified
    """

    df["station_id"] = df[column_source].apply(lambda x: f"{code}-{x}")
    df["created_date"] = date.today()

    return df


def get_insee_code(city_name):
    """
    Get the INSEE code of a city based on table CONSOLIDATE_STATION

    Params : 
        - city_name : name of the city that we want to know the INSEE code

    Returns : insee_code, string, code INSEE of city_name
    """

    data = {}
    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)

    sql_statement = f"""
    SELECT DISTINCT id 
    FROM CONSOLIDATE_CITY
    WHERE name = ?;
    """

    insee_code = con.execute(sql_statement, [city_name]).fetchone()

    return insee_code[0] if insee_code else None
