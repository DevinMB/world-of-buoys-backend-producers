import os
import requests
import redis
import warnings
from noaa_html_parser import NOAAParser
warnings.filterwarnings("ignore")

from dotenv import load_dotenv
load_dotenv()

class SummaryAggregator:
    STATION_URL = os.getenv("STATION_URL")
    STATION_SET_KEY = os.getenv("STATION_SET_KEY")
    
    def __init__(self, redis_conn):
        self.redis_conn = redis_conn

    def fetch_and_store_buoy_data(self):
        available_files_by_station = NOAAParser.fetch_directory_file_list()

        response = requests.get(self.STATION_URL)
        data = response.text
        
        lines = data.splitlines()

        for line in lines:
            if line.strip() == "" or line.startswith("#"):
                continue
            
            parts = line.split("|")
            
            station_id = parts[0].strip()
            owner = parts[1].strip()
            ttype = parts[2].strip()
            hull = parts[3].strip()
            name = parts[4].strip()
            payload = parts[5].strip()
            location = parts[6].strip()
            timezone = parts[7].strip() if len(parts) > 7 else ""
            forecast = parts[8].strip() if len(parts) > 8 else ""
            note = parts[9].strip() if len(parts) > 9 else ""

            self.redis_conn.sadd(self.STATION_SET_KEY, station_id)
            
            summary_key = f"buoy:{station_id}:summary"
            self.redis_conn.hset(summary_key, mapping={
                "owner": owner,
                "ttype": ttype,
                "hull": hull,
                "name": name,
                "payload": payload,
                "location": location,
                "timezone": timezone,
                "forecast": forecast,
                "note": note
            })

            available_info_key = f"buoy:{station_id}:available_info"
            file_types = available_files_by_station.get(station_id, [])
            if file_types:
                self.redis_conn.sadd(available_info_key, *file_types)

        print(f"Job complete. Awaiting next job.")