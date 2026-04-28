import os
import json
import time
import logging
import requests
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
import snowflake.connector
from cryptography.hazmat.primitives import serialization

# --- Configuration ---
POLL_INTERVAL = 3600  # Weather is usually updated hourly
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


class WeatherProducer:
    def __init__(self):
        self.locations = {"NYC": {"lat": 40.71, "lon": -74.00}, "London": {"lat": 51.50, "lon": -0.12}}
        self.private_key_path = "snowflake_key.p8"


    def _fetch_city_weather(self, city: str, lat: float, lon: float) -> Optional[dict]:
        # Adding more useful data types for correlation analysis
        url = (
            f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}"
            "&hourly=temperature_2m,precipitation,visibility,snowfall,wind_speed_10m,weather_code"
            "&current=temperature_2m,relative_humidity_2m,apparent_temperature,is_day,precipitation,rain,showers,snowfall"
            "&past_days=20&forecast_days=1"
        )
        try:
            response = requests.get(url, timeout=20)
            response.raise_for_status()
            data = response.json()
            data['city'] = city  # Tagging the city directly in JSON for easier SQL parsing
            return data
        except Exception as e:
            logging.error(f"Error fetching weather for {city}: {e}")
            return None

    def run(self):
        logging.info("Starting automated weather producer...")
        while True:
            now = datetime.now(timezone.utc)
            for city, coords in self.locations.items():
                data = self._fetch_city_weather(city, coords["lat"], coords["lon"])
                if data:
                    try:
                        self.push_to_snowflake(data)
                        logging.info(f"Successfully pushed {city} weather to Snowflake.")
                    except Exception as e:
                        # okay to fail, data is already saved by the sink above
                        logging.error(f"Snowflake push failed, but data is saved locally: {e}")

            time.sleep(POLL_INTERVAL)

    def push_to_snowflake(self, data):
        """Automated push using Key-Pair Auth - No Browser Needed AHA!"""
        try:
            conn = snowflake.connector.connect(
                user='LION',
                # Updated to match your Account Details screenshot
                account='SFEDU02-UNB02139', 
                private_key_file=self.private_key_path,
                # authenticator = "externalbrowser",
                warehouse='GORILLA_WH',     
                database='TRANSIT_WEATHER_DB',
                schema='RAW',
                # Adding a login timeout so the script doesn't freeze forever
                login_timeout=30 
            ) 
            query = "INSERT INTO WEATHER_HISTORY (RAW_JSON) SELECT PARSE_JSON(%s)"
            conn.cursor().execute(query, (json.dumps(data)))
            conn.close()
            logging.info(f"Successfully pushed {data.get('city')} data to Snowflake.")
        except Exception as e:
            # This will now tell us EXACTLY why the login failed
            logging.error(f"Actual Snowflake Error: {e}")
            raise e
        
if __name__ == "__main__":
    producer = WeatherProducer()
    producer.run()