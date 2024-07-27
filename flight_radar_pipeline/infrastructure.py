import os
from pyspark.sql import SparkSession
from FlightRadar24.api import FlightRadar24API
import logging

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")


def get_spark_session(name: str) -> SparkSession:
    """
    Create a SparkSession

    Args:
        name(str): The name for the SparkSession
    """
    return SparkSession.builder.appName(name).getOrCreate()


def get_flight_radar_api() -> FlightRadar24API:
    """
    Create an instance of FlightRadarAPI
    """
    return FlightRadar24API()


def get_logger(name: str):
    logging.basicConfig(
        level=LOG_LEVEL,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%d/%m/%Y %H:%M:%S",
    )

    return logging.getLogger(name)
