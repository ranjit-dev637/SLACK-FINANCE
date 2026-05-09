import schedule
import time
from loguru import logger
from config import settings
from ezee_client import EZeeClient
from processor import KPIProcessor
from report import ReportGenerator
from database import init_db

def run_agent_job():
    logger.info("Starting scheduled data fetch from eZee Absolute...")
    client = EZeeClient()
    raw_data = client.get_real_time_data()
    
    if not raw_data:
        logger.error("Failed to fetch data, skipping processing cycle.")
        return

    for property_name in settings.PROPERTIES:
        logger.info(f"Processing data for {property_name}...")
        kpis = KPIProcessor.process(raw_data, property_name)
        ReportGenerator.save_and_generate(property_name, kpis)

def main():
    logger.add("ezee_agent.log", rotation="10 MB", level=settings.LOG_LEVEL)
    logger.info("Antigravity Hotel Operations Agent starting up...")
    
    # Initialize database tables
    init_db()
    
    # Run once immediately on start
    run_agent_job()
    
    # Schedule to run every 2 hours
    schedule.every(2).hours.do(run_agent_job)
    logger.info("Scheduler configured: Running every 2 hours.")
    
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    main()
