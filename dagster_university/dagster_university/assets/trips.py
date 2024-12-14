import requests
from dagster_duckdb import DuckDBResource
from . import constants
import duckdb
import os
from dagster import asset


@asset
def taxi_trips_file() -> None:
    """
    The raw parquet files for the taxi trips dataset. Sourced from the NYC Open Data portal.
    """
    month_to_fetch = "2023-03"
    raw_trips = requests.get(
        f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month_to_fetch}.parquet"
    )

    with open(constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch), "wb") as output_file:
        output_file.write(raw_trips.content)


@asset
def taxi_zones_file() -> None:
    """
    The raw CSV file for the taxi zones dataset. Sourced from the NYC Open Data portal.
    """
    raw_taxi_zones = requests.get(f"https://data.cityofnewyork.us/api/views/755u-8jsi/rows.csv?accessType=DOWNLOAD")

    with open(constants.TAXI_ZONES_FILE_PATH, "wb") as output_file:
        output_file.write(raw_taxi_zones.content)


@asset(deps=["taxi_trips_file"])
def taxi_trips(database: DuckDBResource) -> None:
    """
    The raw taxi trips dataset, loaded into a DuckDB database
    """
    query = """
        CREATE OR REPLACE TABLE trips AS (
            SELECT
                VendorID AS vendor_id,
                PULocationID AS pickup_zone_id,
                DOLocationID AS dropoff_zone_id,
                RatecodeID AS rate_code_id,
                payment_type AS payment_type,
                tpep_dropoff_datetime AS dropoff_datetime,
                tpep_pickup_datetime AS pickup_datetime,
                trip_distance AS trip_distance,
                passenger_count AS passenger_count,
                total_amount AS total_amount
            FROM 'data/raw/taxi_trips_2023-03.parquet'
        );
    """

    with database.get_connection() as conn:
        conn.execute(query)


@asset(deps=["taxi_zones_file"])
def taxi_zones() -> None:
    sql_query = f"""
        create or replace table zones as (
            select
                LocationID as zone_id,
                zone,
                borough,
                the_geom as geometry
            from '{constants.TAXI_ZONES_FILE_PATH}'
        );
    """

    conn = duckdb.connect(os.getenv("DUCKDB_DATABASE"))
    conn.execute(sql_query)
