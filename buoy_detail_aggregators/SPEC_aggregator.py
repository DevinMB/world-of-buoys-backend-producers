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


class SPECAggregator:
    BASE_URL = "https://www.ndbc.noaa.gov/data/realtime2/"

    def __init__(self, redis_conn):
        self.redis_conn = redis_conn

    def get_buoy_stations(self):
        return self.redis_conn.smembers("buoy:stations")

    def fetch_and_store_buoy_data(self, station_id):
        url = f"{self.BASE_URL}{station_id}.spec"

        try:
            response = requests.get(url)
            response.raise_for_status()
        except Exception as e:
            print(f"HTTP error occurred while fetching data for {station_id}: {e}")
            return 0  

        lines = response.text.splitlines()
        key = f"buoy:{station_id}:spec-data"

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
                    self.redis_conn.zadd(key, {json.dumps(data): timestamp})
                    records_processed_for_station += 1
                else:
                    break

        return records_processed_for_station

    def parse_buoy_data_line(self, line):
        parts = line.strip().split()

        if len(parts) < 15:  

            return None
                
        year_str, month_str, day_str, hour_str, minute_str = parts[0:5]

        try:
            year = int(year_str)
            month = int(month_str)
            day = int(day_str)
            hour = int(hour_str)
            minute = int(minute_str)
        except ValueError:
            print(f"Error parsing date and time in line: {line}")
            return None
        
        try:
            dt = datetime(year, month, day, hour, minute)
        except ValueError as e:
            print(f"Invalid date/time in line: {line} - {e}")
            return None

        timestamp = int(dt.timestamp())


        def parse_value(value):
            try:
                return float(value) if value != 'MM' else None
            except ValueError:
                return None  


        wvht = parse_value(parts[5])  # Significant Wave Height (m)
        swell_height = parse_value(parts[6])  # Swell Height (m)
        swell_period = parse_value(parts[7])  # Swell Period (sec)
        wind_wave_height = parse_value(parts[8])  # Wind Wave Height (m)
        wind_wave_period = parse_value(parts[9])  # Wind Wave Period (sec)


        swell_direction = parts[10] if parts[10] != 'MM' else None  # Swell Direction
        wind_wave_direction = parts[11] if parts[11] != 'MM' else None  # Wind Wave Direction


        steepness = parts[12] if parts[12] != 'MM' else None  # Steepness
        average_period = parse_value(parts[13])  # Average Period (sec)
        mean_wave_direction = parse_value(parts[14])  # Mean Wave Direction (deg)

        data = {
            "WVHT": wvht,
            "swellHeight": swell_height,
            "swellPeriod": swell_period,
            "windWaveHeight": wind_wave_height,
            "windWavePeriod": wind_wave_period,
            "swellDirection": swell_direction,  
            "windWaveDirection": wind_wave_direction,  
            "steepness": steepness,
            "averagePeriod": average_period,
            "meanWaveDirection": mean_wave_direction
        }

        return timestamp, data


    def run(self):
        buoy_stations = self.get_buoy_stations()
        records_processed = 0
        total_stations = len(buoy_stations)
        current_buoy_count = 0

        for station_id in buoy_stations:
            available_info_key = f"buoy:{station_id}:available_info"
            if not self.redis_conn.sismember(available_info_key, 'spec'):
                current_buoy_count += 1
                continue
            time.sleep(REQUEST_DELAY)
            current_buoy_count += 1
            records_processed += self.fetch_and_store_buoy_data(station_id)
            print(f"{current_buoy_count}/{total_stations} Buoys processed.")
        print(f"Aggregation Job Complete. Awaiting Next Job.")
        return records_processed
