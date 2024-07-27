from pyspark.sql import DataFrame, Row
from pyspark.sql.window import Window
from pyspark.sql.functions import udf, col, count, row_number


def clean_data(dataframe: DataFrame) -> DataFrame:
    """
    Supprime tous les valeurs vide
    """
    cl_dataframe = dataframe.na.replace(["N/A", "NaN", ""], None).dropna()
    return cl_dataframe


def get_udf_continent(zones):
    """
    La functionne UDF pour determiner continent a partir des coordonees
    """

    def get_continent(latitude, longitude):
        for continent, value in zones.items():
            if (
                value["tl_x"] <= latitude <= value["br_x"]
                and value["br_y"] <= longitude <= value["tl_y"]
            ):
                return continent
        else:
            return "other"

    return udf(get_continent)


def get_most_active_airline(dataframe: DataFrame) -> DataFrame:
    """
    La compagnie avec le + de vols en cours
    """
    grouped_active_flights = (
        dataframe.filter("on_ground==0")
        .groupBy("airline_icao")
        .agg({"id": "count"})
        .withColumnRenamed("count(id)", "count")
    )
    max_count = grouped_active_flights.agg({"count": "max"}).collect()[0][0]
    most_active_airline = grouped_active_flights.filter(col("count") == max_count)

    return most_active_airline


def get_most_active_airline_per_continent(dataframe: DataFrame, zones) -> DataFrame:
    """
    Pour chaque continent, la compagnie avec le + de vols régionaux actifs (continent d'origine == continent de destination)
    """
    udf_get_continent = get_udf_continent(zones)
    grouped_regional_flights = (
        dataframe.filter((col("on_ground") == 0))
        .withColumn(
            "continent",
            udf_get_continent(col("latitude"), col("longitude")),
        )
        .groupBy(["continent", "airline_icao"])
        .agg(count("id").alias("count"))
    )

    partition = Window.partitionBy("continent").orderBy(col("count").desc())

    most_active_airlines = (
        grouped_regional_flights.withColumn("row_number", row_number().over(partition))
        .filter(col("row_number") == 1)
        .drop("row_number")
    )

    return most_active_airlines


def get_longest_ongoing_flight(dataframe: DataFrame) -> Row:
    """
    Le vol en cours avec le trajet le plus long
    """
    longest_flight = dataframe.orderBy(col("time").desc()).first()

    return longest_flight


def get_avg_flight_duration_per_continent(dataframe: DataFrame, zones):
    """
    Pour chaque continent, la longueur de vol moyenne
    """
    udf_get_continent = get_udf_continent(zones)
    average_flight_duration_per_continent = (
        dataframe.withColumn(
            "continent",
            udf_get_continent(col("latitude"), col("longitude")),
        )
        .groupBy("continent")
        .avg("time")
        .withColumnRenamed("avg(time)", "average_duration")
    )

    return average_flight_duration_per_continent


def get_aircraft_manufacturer_with_most_active_flights(
    dataframe: DataFrame,
) -> Row:
    """
    L'entreprise constructeur d'avions avec le plus de vols actifs
    """
    most_active_aircraft = (
        dataframe.filter("on_ground == 0")
        .groupBy("aircraft_code")
        .agg(count("id").alias("count"))
        .orderBy(col("count").desc())
        .first()
    )

    return most_active_aircraft


def get_top_3_aircraft_models_per_airline(dataframe: DataFrame) -> DataFrame:
    """
    Pour chaque pays de compagnie aérienne, le top 3 des modèles d'avion en usage
    """
    top_aircrafts_per_airline = (
        dataframe.groupBy(["airline_iata", "aircraft_code"])
        .agg(count("id").alias("count"))
        .orderBy(col("count").desc())
    )

    partition = Window.partitionBy("airline_iata").orderBy(col("count").desc())

    top_3_aircrafts_per_airline = (
        top_aircrafts_per_airline.withColumn("row_number", row_number().over(partition))
        .filter(col("row_number") <= 3)
        .drop("row_number")
    )

    return top_3_aircrafts_per_airline
