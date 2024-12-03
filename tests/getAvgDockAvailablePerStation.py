import duckdb

def testGetAvgDockAvailablePerStation():
    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)

    sql_statement = """
    SELECT ds.name, ds.code, ds.address, tmp.avg_dock_available
    FROM DIM_STATION ds JOIN (
        SELECT station_id, AVG(BICYCLE_AVAILABLE) AS avg_dock_available
        FROM FACT_STATION_STATEMENT
        GROUP BY station_id
    ) AS tmp ON ds.id = tmp.station_id;
    """

    result = con.execute(sql_statement).df()

    print(result)

testGetAvgDockAvailablePerStation()