from data_agregation import (
    create_agregate_tables,
    agregate_dim_city,
    agregate_dim_station,
    agregate_fact_station_statements
)
from data_consolidation import (
    create_consolidate_tables,
    consolidate_city_data,
    consolidate_station_data,
    consolidate_station_statement_data,
)
from data_ingestion import (
    get_paris_realtime_bicycle_data,
    get_nantes_realtime_bicycle_data,
    get_toulouse_realtime_bicycle_data,
    get_strasbourg_realtime_bicycle_data,
    get_commune_data
)

def main():
    print("Process start.")

    # data ingestion
    print("------------------------------------")
    print("Data ingestion started.")

    # For Paris
    print("Starting Paris ingestion...")
    get_paris_realtime_bicycle_data()
    print("Paris' ingestion done !")

    # For Nantes
    print("Starting Nantes ingestion...")
    get_nantes_realtime_bicycle_data()
    print("Nantes' ingestion done !")

    # For Toulouse
    print("Starting Toulouse ingestion...")
    get_toulouse_realtime_bicycle_data()
    print("Toulouse's ingestion done !")

    # For Strasbourg
    print("Starting Strasbourg ingestion...")
    get_strasbourg_realtime_bicycle_data()
    print("Strasbourg's ingestion done !")

    # Towns data ingestion
    print("Starting towns ingestion...")
    get_commune_data()
    print("Towns' ingestion done !")

    print("Data ingestion done !")
    print("------------------------------------")

    # data consolidation
    print("Consolidation data started.")
    create_consolidate_tables()
    consolidate_city_data()
    consolidate_station_data()
    consolidate_station_statement_data()
    print("Consolidation data ended.")
    print("------------------------------------")

    # data agregation
    print("Agregate data started.")
    create_agregate_tables()
    agregate_dim_city()
    agregate_dim_station()
    agregate_fact_station_statements()
    print("Agregate data ended.")

if __name__ == "__main__":
    main()