import redis
import os
import logging
import json_log_formatter
import time
import sys
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
from summary_aggregator import SummaryAggregator
from dotenv import load_dotenv
import warnings
from urllib3.exceptions import NotOpenSSLWarning

load_dotenv()

warnings.filterwarnings("ignore", category=NotOpenSSLWarning)


formatter = json_log_formatter.JSONFormatter()
json_handler = logging.StreamHandler()
json_handler.setFormatter(formatter)

logger = logging.getLogger()
logger.addHandler(json_handler)
logger.setLevel(logging.INFO)

REDIS_SERVER = os.getenv("REDIS_SERVER")

def run_summary_aggregator():
    logging.info("SummaryAggregator started running.")
    start_time = time.time()

    redis_conn = redis.Redis(host=REDIS_SERVER, port=6379, decode_responses=True)
    aggregator = SummaryAggregator(redis_conn)
    records_processed = aggregator.fetch_and_store_buoy_data()
    
    elapsed_time = time.time() - start_time
    hours, remainder = divmod(elapsed_time, 3600)
    minutes, seconds = divmod(remainder, 60)

    logging.info(f"SummaryAggregator finished successfully. Elapsed time: {int(hours)}h {int(minutes)}m {int(seconds)}s. {records_processed} records were processed.")
    print(f"Job complete, {records_processed} records processed. Awaiting next job.")

def main():
    try:
        print("Application Running...")
        scheduler = BlockingScheduler()

        logging.info("Running SummaryAggregator on startup.")
        run_summary_aggregator()

        scheduler.add_job(run_summary_aggregator, 'cron', hour=0, minute=0)

        logging.info("Scheduler started. Services will run on their schedules.")
        scheduler.start()


    except Exception as e:
        logging.error(f"An error occurred: {e}", exc_info=True)
        print("Application encountered an error, please see log file. Exiting.")
if __name__ == "__main__":
    main()