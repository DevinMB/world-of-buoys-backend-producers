import redis
import os
import requests
import json
from dotenv import load_dotenv
load_dotenv()

REDIS_SERVER = os.getenv("REDIS_SERVER")
redis_conn = redis.Redis(host=REDIS_SERVER, port=6379, decode_responses=True)

class BuoyDataAggregator:
    BASE_URL = "https://www.ndbc.noaa.gov/data/realtime2/"

    def __init__(self, redis_conn):
        self.redis_conn = redis_conn

    def get_buoy_stations(self):
        return self.redis_conn.smembers("buoy:stations")

    def fetch_and_store_buoy_data(self, station_id):
        url = f"{self.BASE_URL}{station_id}.txt"
        print(f"Fetching data from {url}")

        response = requests.get(url)
        if response.status_code != 200:
            print(f"Failed to fetch data for {station_id}")
            return

        lines = response.text.splitlines()
        for line in lines:
            if line.startswith('#') or not line.strip():
                continue
            
            parsed_data = self.parse_buoy_data_line(line)
            if parsed_data:
                timestamp, data = parsed_data
                self.store_buoy_data(station_id, timestamp, data)

    def parse_buoy_data_line(self, line):
        parts = line.split()

        if len(parts) < 18:
            return None

        year, month, day, hour, minute = parts[0], parts[1], parts[2], parts[3], parts[4]
        timestamp = f"{year}{month}{day}{hour}{minute}"

        data = {
            "WDIR": parts[5],    # Wind Direction
            "WSPD": parts[6],    # Wind Speed
            "GST": parts[7],     # Gust Speed
            "WVHT": parts[8],    # Wave Height
            "DPD": parts[9],     # Dominant Wave Period
            "APD": parts[10],    # Average Wave Period
            "MWD": parts[11],    # Mean Wave Direction
            "PRES": parts[12],   # Pressure
            "ATMP": parts[13],   # Air Temperature
            "WTMP": parts[14],   # Water Temperature
            "DEWP": parts[15],   # Dew Point
            "VIS": parts[16],    # Visibility
            "PTDY": parts[17],   # Pressure Tendency
            "TIDE": parts[18] if len(parts) > 18 else "MM"  # Tide
        }

        return timestamp, data

    def store_buoy_data(self, station_id, timestamp, data):
        key = f"buoy:{station_id}:standard-data"
        self.redis_conn.zadd(key, {json.dumps(data): timestamp})

    def run(self):
        buoy_stations = self.get_buoy_stations()
        for station_id in buoy_stations:
            self.fetch_and_store_buoy_data(station_id)


# Example usage
if __name__ == "__main__":
    aggregator = BuoyDataAggregator(redis_conn)
    aggregator.run()
