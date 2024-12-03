import duckdb

def testGetSumDocksAvailablePerCity():
    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)

    sql_statement = """
    SELECT dm.NAME, tmp.SUM_BICYCLE_DOCKS_AVAILABLE
    FROM DIM_CITY dm INNER JOIN (
        SELECT CITY_ID, SUM(BICYCLE_DOCKS_AVAILABLE) AS SUM_BICYCLE_DOCKS_AVAILABLE
        FROM FACT_STATION_STATEMENT
        WHERE CREATED_DATE = (SELECT MAX(CREATED_DATE) FROM CONSOLIDATE_STATION)
        GROUP BY CITY_ID
    ) tmp ON dm.ID = tmp.CITY_ID
    WHERE lower(dm.NAME) in ('paris', 'nantes', 'vincennes', 'toulouse', 'strasbourg', 'montpellier');
    """

    result = con.execute(sql_statement).df()

    print(result)

testGetSumDocksAvailablePerCity()