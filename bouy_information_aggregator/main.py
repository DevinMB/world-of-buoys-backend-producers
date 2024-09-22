import redis
import os
import logging
import time
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
from summary_aggregator import SummaryAggregator
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

def run_summary_aggregator():
    logging.info("SummaryAggregator started running.")
    start_time = time.time()

    redis_conn = redis.Redis(host=REDIS_SERVER, port=6379, decode_responses=True)
    aggregator = SummaryAggregator(redis_conn)
    aggregator.fetch_and_store_buoy_data()
    
    elapsed_time = time.time() - start_time
    logging.info(f"SummaryAggregator finished successfully. Elapsed time: {elapsed_time:.2f} seconds.")


def main():
    try:
        print("Application Running...")
        scheduler = BlockingScheduler()

        logging.info("Running SummaryAggregator on startup.")
        run_summary_aggregator()

        scheduler.add_job(run_summary_aggregator, 'cron', hour=0, minute=0)

        # Schedule other services here (for future tasks)
        # scheduler.add_job(run_other_service, 'interval', hours=6)

        logging.info("Scheduler started. Services will run on their schedules.")
        scheduler.start()


    except Exception as e:
        logging.error(f"An error occurred: {e}", exc_info=True)
        print("Applicatino encountered an error, please see log file. Exiting.")
if __name__ == "__main__":
    main()