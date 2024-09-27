import redis
import os
import logging
import time
from apscheduler.schedulers.blocking import BlockingScheduler
from SMD_aggregator import SMDAggregator
from WCH_aggregator import WCHAggregator

from dotenv import load_dotenv
import warnings
from urllib3.exceptions import NotOpenSSLWarning

load_dotenv()

warnings.filterwarnings("ignore", category=NotOpenSSLWarning)

logging.basicConfig(
    filename='app.log', 
    level=logging.INFO,  
    format='%(asctime)s - %(levelname)s - %(message)s',
)

REDIS_SERVER = os.getenv("REDIS_SERVER")

aggregator_classes = {
    'SMD': SMDAggregator,
    'WCH': WCHAggregator
}

AGGREGATOR_TYPE = os.getenv("AGGREGATOR_TYPE")

print(AGGREGATOR_TYPE)
def run_aggregator():
    records_processed = 0
    logging.info(f"{AGGREGATOR_TYPE} Aggregator has begun running.")
    start_time = time.time()

    redis_conn = redis.Redis(host=REDIS_SERVER, port=6379, decode_responses=True)

    AggregatorClass = aggregator_classes.get(AGGREGATOR_TYPE)

    if AggregatorClass is None:
        logging.error(f"Unknown aggregator type: {AGGREGATOR_TYPE}")
        return 

    aggregator = AggregatorClass(redis_conn)
    records_processed = aggregator.run()

    elapsed_time = time.time() - start_time
    hours, remainder = divmod(elapsed_time, 3600)
    minutes, seconds = divmod(remainder, 60)

    logging.info(f"{AGGREGATOR_TYPE} Aggregator finished successfully. Elapsed time: {int(hours)}h {int(minutes)}m {int(seconds)}s. {records_processed} records were processed.")

def main():
    try:
        print("Application Running...")
        scheduler = BlockingScheduler()

        # This guy takes an hour to load everything - comment out for normal testing
        logging.info(f"Running {AGGREGATOR_TYPE} Aggregator on startup.") 
        run_aggregator()

        scheduler.add_job(run_aggregator, 'interval', days=1)

        logging.info("Scheduler started. Services will run on their schedules.")
        scheduler.start()


    except Exception as e:
        logging.error(f"An error occurred: {e}", exc_info=True)
        print("Application encountered an error, please see log file. Exiting.")
if __name__ == "__main__":
    main()