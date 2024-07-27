import os
from flight_radar_pipeline.infrastructure import (
    get_flight_radar_api,
    get_logger,
    get_spark_session,
)
from flight_radar_pipeline.extract import get_flights, get_zones
from flight_radar_pipeline.transform import (
    clean_data,
    get_aircraft_manufacturer_with_most_active_flights,
    get_avg_flight_duration_per_continent,
    get_longest_ongoing_flight,
    get_most_active_airline,
    get_most_active_airline_per_continent,
    get_top_3_aircraft_models_per_airline,
)
from flight_radar_pipeline.load import write_to_csv

SPARK_SESSION_NAME = os.environ.get("SPARK_SESSION_NAME", "FlightRadar24")
APPLICATION_NAME = os.environ.get("APPLICATION_NAME", "FlightRadar24")

FLIGHTS_DIR = os.environ.get("FLIGHTS_DIR", "Flights")

STG_LAYER = os.environ.get("STG_LAYER", "output/stg")
ODS_LAYER = os.environ.get("ODS_LAYER", "output/ods")
CDM_LAYER = os.environ.get("CDM_LAYER", "output/cdm")


def flight_radar_ETL():
    # Create logger
    logger = get_logger(APPLICATION_NAME)

    logger.info("Start ETL job")

    # Initialize the FlightRadar24 API
    fr_api = get_flight_radar_api()
    # Create a SparkSession
    spark = get_spark_session(SPARK_SESSION_NAME)

    # Get the flights DataFrame & zones dict
    zones = get_zones(fr_api)
    flights_raw = run_get_flights(fr_api, spark, logger)
    flights = run_get_clean_flights(flights_raw, logger)

    logger.info("Transform & load gold data")
    run_most_active_airline(flights, logger)
    run_most_active_airline_per_continent(flights, zones, logger)
    run_longest_flight(flights, spark, logger)
    run_avg_flight_duration_per_continent(flights, zones, logger)
    run_aircraft_manufacturer(flights, spark, logger)
    run_top_3_aircraft_models(flights, logger)

    # Stop the SparkSession
    spark.stop()

    logger.info("ETL job executed successfully")


def run_get_flights(fr_api, spark, logger):
    """
    Get the flights DataFrame

    Bronze layer
    """
    try:
        logger.info("Extract & load bronze")
        flights = get_flights(fr_api)

        flights_raw = spark.createDataFrame(data=flights)
        # Save to bronze layer
        write_to_csv(flights_raw, f"{STG_LAYER}/{FLIGHTS_DIR}", "flights")
        return flights_raw
    except:
        logger.exception("Cannot fetch FlightRadar data")
        raise


def run_get_clean_flights(flights_raw, logger):
    """
    Clean flights DataFrame

    Silver layer
    """
    try:
        logger.info("Clean & load silver")
        flights = clean_data(flights_raw)

        # Save to silver layer
        write_to_csv(flights, f"{ODS_LAYER}/{FLIGHTS_DIR}", "flights")

        return flights
    except:
        logger.exception("Cannot clean FlightRadar data")
        raise


def run_most_active_airline(flights, logger):
    """
    La compagnie avec le + de vols en cours

    Gold layer
    """
    try:
        most_active_airline = get_most_active_airline(flights)
        write_to_csv(
            most_active_airline, f"{CDM_LAYER}/{FLIGHTS_DIR}", "most_active_airline"
        )
    except:
        logger.exception("Cannot get most active airline")


def run_most_active_airline_per_continent(flights, udf_get_continent, logger):
    """
    Pour chaque continent, la compagnie avec le + de vols régionaux actifs (continent d'origine == continent de destination)

    Gold layer
    """
    try:
        most_active_airline_per_continent = get_most_active_airline_per_continent(
            flights, udf_get_continent
        )
        write_to_csv(
            most_active_airline_per_continent,
            f"{CDM_LAYER}/{FLIGHTS_DIR}",
            "most_active_airline_per_continent",
        )
    except:
        logger.exception("Cannot get most active airline per continent")


def run_longest_flight(flights, spark, logger):
    """
    Le vol en cours avec le trajet le plus long

    Gold layer
    """
    try:
        longest_flight = get_longest_ongoing_flight(flights)
        longest_flight = spark.createDataFrame(data=[longest_flight])
        write_to_csv(longest_flight, f"{CDM_LAYER}/{FLIGHTS_DIR}", "longest_flight")
    except:
        logger.exception("Cannot get longest ongoing flight")


def run_avg_flight_duration_per_continent(flights, udf_get_continent, logger):
    """
    Pour chaque continent, la longueur de vol moyenne

    Gold layer
    """
    try:
        avg_flight_duration_per_continent = get_avg_flight_duration_per_continent(
            flights, udf_get_continent
        )
        write_to_csv(
            avg_flight_duration_per_continent,
            f"{CDM_LAYER}/{FLIGHTS_DIR}",
            "avg_flight_duration_per_continent",
        )
    except:
        logger.exception("Cannot get avg flight duration per continent")


def run_aircraft_manufacturer(flights, spark, logger):
    """
    L'entreprise constructeur d'avions avec le plus de vols actifs

    Gold layer
    """
    try:
        aircraft_manufacturer = get_aircraft_manufacturer_with_most_active_flights(
            flights
        )
        aircraft_manufacturer = spark.createDataFrame(data=[aircraft_manufacturer])
        write_to_csv(
            aircraft_manufacturer, f"{CDM_LAYER}/{FLIGHTS_DIR}", "aircraft_manufacturer"
        )
    except:
        logger.exception("Cannot get aircraft manufacturer with most active flights")


def run_top_3_aircraft_models(flights, logger):
    """
    Pour chaque pays de compagnie aérienne, le top 3 des modèles d'avion en usage

    Gold layer
    """
    try:
        top_3_aircraft_models = get_top_3_aircraft_models_per_airline(flights)
        write_to_csv(
            top_3_aircraft_models, f"{CDM_LAYER}/{FLIGHTS_DIR}", "top_3_aircraft_models"
        )
    except:
        logger.exception("Cannot get top 3 aircraft models per airline")
