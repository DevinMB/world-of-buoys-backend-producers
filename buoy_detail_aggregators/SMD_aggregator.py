import redis
import os
import requests
import json
import time
from dotenv import load_dotenv
load_dotenv()

REDIS_SERVER = os.getenv("REDIS_SERVER")
REQUEST_DELAY = float(os.getenv("REQUEST_DELAY"))

redis_conn = redis.Redis(host=REDIS_SERVER, port=6379, decode_responses=True)

class SMDAggregator:
    BASE_URL = "https://www.ndbc.noaa.gov/data/realtime2/"

    def __init__(self, redis_conn):
        self.redis_conn = redis_conn

    def get_buoy_stations(self):
        return self.redis_conn.smembers("buoy:stations")

    def fetch_and_store_buoy_data(self, station_id):
        url = f"{self.BASE_URL}{station_id}.txt"
        # print(f"Fetching data from {url}")

        try:
            response = requests.get(url)
            response.raise_for_status()
        except Exception as e:
            print(f"HTTP error occurred while fetching data for {station_id}: {e}")
            return 0  # Skip processing

        lines = response.text.splitlines()
        key = f"buoy:{station_id}:standard-data"

        records_processed_for_station = 0

        for line in lines:
            if line.startswith('#') or not line.strip():
                continue

            parsed_data = self.parse_buoy_data_line(line)
            if parsed_data:
                timestamp_str, data = parsed_data
                timestamp = float(timestamp_str)
                count = self.redis_conn.zcount(key, timestamp, timestamp)
                if count == 0:
                    # Timestamp does not exist, store the data
                    self.redis_conn.zadd(key, {json.dumps(data): timestamp})
                    records_processed_for_station = records_processed_for_station + 1
                else:
                    break  
        
        return records_processed_for_station      


    def parse_buoy_data_line(self, line):
        parts = line.split()

        if len(parts) < 18:
            return None

        year, month, day, hour, minute = parts[0], parts[1], parts[2], parts[3], parts[4]
        timestamp = f"{year}{month}{day}{hour}{minute}"

        data = {
            "windDirection": parts[5],    # Wind Direction
            "windSpeed": parts[6],    # Wind Speed
            "gustSpeed": parts[7],     # Gust Speed
            "waveHeight": parts[8],    # Wave Height
            "dominantWavePeriod": parts[9],     # Dominant Wave Period
            "averageWavePeriod": parts[10],    # Average Wave Period
            "meanWaveDirection": parts[11],    # Mean Wave Direction
            "pressure": parts[12],   # Pressure
            "airTemperature": parts[13],   # Air Temperature
            "waterTemperature": parts[14],   # Water Temperature
            "dewPoint": parts[15],   # Dew Point
            "visibility": parts[16],    # Visibility
            "pressureTendency": parts[17],   # Pressure Tendency
            "tide": parts[18] if len(parts) > 18 else "MM"  # Tide
        }

        return timestamp, data


    def run(self):
        buoy_stations = self.get_buoy_stations()
        records_processed = 0
        total_stations = len(buoy_stations)
        current_buoy_count = 0
        for station_id in buoy_stations:
            available_info_key = f"buoy:{station_id}:available_info"
            if not self.redis_conn.sismember(available_info_key, 'txt'):
                # print(f"Skipping {station_id} - .txt file not available.")
                current_buoy_count += 1
                continue
            time.sleep(REQUEST_DELAY)
            current_buoy_count += 1
            records_processed = records_processed + self.fetch_and_store_buoy_data(station_id)
            print(f"{current_buoy_count}/{total_stations} Buoys processed.")
        print(f"Aggregation Job Complete. Awaiting Next Job.")
        return records_processed
