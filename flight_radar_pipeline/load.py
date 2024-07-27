from datetime import datetime
from pyspark.sql import DataFrame


def write_to_csv(dataframe: DataFrame, folder: str, prefix: str):
    """
    Write DataFrame to a CSV file

    Args:
        dataframe (DataFrame): DataFrame to be written
        folder (str): Location folder name
        prefix (str):  Output files prefix
    """
    timestamp = datetime.now()
    year = timestamp.strftime("%Y")
    month = timestamp.strftime("%Y-%m")
    day = timestamp.strftime("%Y-%m-%d")
    formatted_timestamp = timestamp.strftime("%Y%m%d%H%M%S%f")[
        :-3
    ]  # Remove last 3 digits of microseconds

    output_path = f"{folder}/rawzone/tech_year={year}/tech_month={month}/tech_day={day}/{prefix}{formatted_timestamp}"
    dataframe.write.csv(output_path, header=True, mode="overwrite")
