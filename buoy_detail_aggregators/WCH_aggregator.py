import redis
import os
import requests
import json
import time
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()

REDIS_SERVER = os.getenv("REDIS_SERVER")
REQUEST_DELAY = float(os.getenv("REQUEST_DELAY"))

redis_conn = redis.Redis(host=REDIS_SERVER, port=6379, decode_responses=True)

class WCHAggregator:
    BASE_URL = "https://www.ndbc.noaa.gov/data/realtime2/"

    def __init__(self, redis_conn):
        self.redis_conn = redis_conn

    def get_buoy_stations(self):
        return self.redis_conn.smembers("buoy:stations")

    def fetch_and_store_buoy_data(self, station_id):
        url = f"{self.BASE_URL}{station_id}.dart"  
        # print(f"Fetching data from {url}")

        try:
            response = requests.get(url)
            response.raise_for_status()
        except Exception as e:
            print(f"HTTP error occurred while fetching data for {station_id}: {e}")
            return 0  # Skip processing

        lines = response.text.splitlines()
        key = f"buoy:{station_id}:wch" 

        records_processed_for_station = 0

        for line in lines:
            if line.startswith('#') or not line.strip():
                continue

            parsed_data = self.parse_buoy_data_line(line)
            if parsed_data:
                timestamp, data = parsed_data
                count = self.redis_conn.zcount(key, timestamp, timestamp)
                if count == 0:
                    self.redis_conn.zadd(key, {json.dumps(data): timestamp})
                    records_processed_for_station += 1
                else:
                    break

        return records_processed_for_station      

    def parse_buoy_data_line(self, line):
        parts = line.strip().split()

        if len(parts) < 8:
            return None

        year_str, month_str, day_str, hour_str, minute_str, second_str = parts[0:6]
        T = parts[6]
        HEIGHT = parts[7]

        try:
            year = int(year_str)
            month = int(month_str)
            day = int(day_str)
            hour = int(hour_str)
            minute = int(minute_str)
            second = int(second_str)
        except ValueError:
            print(f"Error parsing date and time in line: {line}")
            return None

        try:
            dt = datetime(year, month, day, hour, minute, second)
        except ValueError as e:
            print(f"Invalid date/time in line: {line} - {e}")
            return None

        timestamp = int(dt.timestamp())

        data = {
            "T": T,   
            "HEIGHT": HEIGHT
        }

        return timestamp, data

    def run(self):
        buoy_stations = self.get_buoy_stations()
        records_processed = 0
        total_stations = len(buoy_stations)
        current_buoy_count = 0
        for station_id in buoy_stations:
            available_info_key = f"buoy:{station_id}:available_info"
            if not self.redis_conn.sismember(available_info_key, 'dart'):
                # print(f"Skipping {station_id} - .dart file not available.")
                current_buoy_count += 1
                continue
            time.sleep(REQUEST_DELAY)
            records_processed += self.fetch_and_store_buoy_data(station_id)
            current_buoy_count += 1
            print(f"{current_buoy_count}/{total_stations} Buoys processed.")
        return records_processed
