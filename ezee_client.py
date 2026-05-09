import requests
from requests.exceptions import RequestException
from config import settings
from loguru import logger
import time
from datetime import datetime, timedelta

class EZeeClient:
    def __init__(self):
        self.base_url = settings.EZEE_BASE_URL
        self.api_key = settings.EZEE_API_KEY
        self.headers = {"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"}

    def fetch_with_retry(self, endpoint: str, max_retries: int = 3):
        url = f"{self.base_url}/{endpoint}"
        for attempt in range(max_retries):
            try:
                # Mocking the actual API call for safety
                # response = requests.get(url, headers=self.headers, timeout=10)
                # response.raise_for_status()
                # return response.json()
                logger.debug(f"Mock fetching from {url} (Attempt {attempt+1})")
                return self._generate_mock_data()
            except RequestException as e:
                logger.warning(f"Attempt {attempt + 1} failed: {e}")
                if attempt == max_retries - 1:
                    logger.error(f"All retries failed for {endpoint}")
                    return None
                time.sleep(2 ** attempt)

    def _generate_mock_data(self):
        # Mock data generation based on the properties requested
        return {
            "Clover Villa": {"bookings": [{"booking_id": "B1", "source": "OTA", "status": "Confirmed", "revenue": 150.0}], "inventory": {"total": 20, "available": 5}},
            "Clovera": {"bookings": [{"booking_id": "B2", "source": "Direct", "status": "Confirmed", "revenue": 100.0}], "inventory": {"total": 30, "available": 10}},
            "Clover Woods": {"bookings": [{"booking_id": "B3", "source": "OTA", "status": "Cancelled", "revenue": 0.0}], "inventory": {"total": 15, "available": 10}},
            "Clover Connect": {"bookings": [{"booking_id": "B4", "source": "Direct", "status": "No Show", "revenue": 0.0}], "inventory": {"total": 40, "available": 20}}
        }

    def get_real_time_data(self):
        logger.info("Fetching real-time data from eZee Absolute")
        return self.fetch_with_retry("get_data")
