import redis
import os
from dotenv import load_dotenv 

load_dotenv()

REDIS_SERVER = os.getenv("REDIS_SERVER", "localhost")
redis_conn = redis.Redis(host=REDIS_SERVER, port=6379, decode_responses=True)

def get_buoys_and_locations():
    buoy_ids = redis_conn.smembers("buoy:stations")
    
    buoys_with_locations = []
    for buoy_id in buoy_ids:
        summary_key = f"buoy:{buoy_id}:summary"
        location = redis_conn.hget(summary_key, "location")
        
        if location:
            buoys_with_locations.append({"buoy_id": buoy_id, "location": location})
    
    return buoys_with_locations

if __name__ == "__main__":
    buoys_locations = get_buoys_and_locations()
    
    for buoy in buoys_locations:
        print(f" Location: {buoy['location']}")
