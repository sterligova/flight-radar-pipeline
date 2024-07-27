import os
import time
import schedule
from flight_radar_pipeline.etl import flight_radar_ETL

SCHEDULER_INTERVAL_MIN = os.environ.get("SCHEDULER_INTERVAL_MIN", 120)

def main():
    # https://schedule.readthedocs.io/en/stable/
    schedule.every(SCHEDULER_INTERVAL_MIN).seconds.do(flight_radar_ETL)

    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    main()
