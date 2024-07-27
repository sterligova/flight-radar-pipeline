from typing import Dict
from FlightRadar24.api import FlightRadar24API


def get_flights(fr_api: FlightRadar24API) -> list:
    """
    Get the list of flights

    Args:
        fr_api(FlightRadar24API): Instance of FlightRadar24API
    """
    flights = fr_api.get_flights()
    return map(
        lambda flight: (
            {
                "id": flight.id,
                "number": flight.number,
                "time": flight.time,
                "altitude": flight.altitude,
                "latitude": flight.latitude,
                "longitude": flight.longitude,
                "vertical_speed": flight.vertical_speed,
                "ground_speed": flight.ground_speed,
                "on_ground": flight.on_ground,
                "heading": flight.heading,
                "registration": flight.registration,
                "icao_24bit": flight.icao_24bit,
                "airline_icao": flight.airline_icao,
                "aircraft_code": flight.aircraft_code,
                "origin_airport_iata": flight.origin_airport_iata,
                "destination_airport_iata": flight.destination_airport_iata,
                "airline_iata": flight.airline_iata,
                "callsign": flight.callsign,
            }
        ),
        flights,
    )


def get_zones(fr_api: FlightRadar24API) -> Dict[str, Dict]:
    """
    Getting zones list From API

    Args:
        fr_api(FlightRadar24API): Instance of FlightRadar24API
    """
    return fr_api.get_zones()
