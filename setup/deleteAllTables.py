import duckdb

def deleteAllTables():
    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)

    tables = ["FACT_STATION_STATEMENT", "CONSOLIDATE_CITY", "CONSOLIDATE_STATION", "CONSOLIDATE_STATION_STATEMENT", "DIM_CITY", "DIM_STATION"]

    for table in tables:
        sql_statement = f"DROP TABLE IF EXISTS {table}"
        con.execute(sql_statement)

deleteAllTables()