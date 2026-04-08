import os
import json
import time
import logging
import requests
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

# --- Configuration ---
POLL_INTERVAL = 3600  # Weather is usually updated hourly
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class DataSink(ABC):
    @abstractmethod
    def write(self, data: Any, metadata: dict) -> None:
        pass

class LocalFileSystemSink(DataSink):
    def __init__(self, base_path: str):
        self.base_path = base_path

    def write(self, data: Any, metadata: dict) -> None:
        data_type = metadata["data_type"]
        city = metadata["city"]
        timestamp = metadata["timestamp"].strftime("%Y%m%d_%H%M%S")

        out_dir = os.path.join(self.base_path, data_type, city)
        os.makedirs(out_dir, exist_ok=True)

        filename = f"{city}_{data_type}_{timestamp}.json"
        filepath = os.path.join(out_dir, filename)

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

class WeatherProducer:
    def __init__(self, sink: DataSink):
        self.sink = sink
        # Coordinates for comparison cities
        self.locations = {
            "NYC": {"lat": 40.71, "lon": -74.00},
            "London": {"lat": 51.50, "lon": -0.12}
        }

    def _fetch_city_weather(self, city: str, lat: float, lon: float) -> Optional[dict]:
        # Adding more useful data types for correlation analysis
        url = (
            f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}"
            "&hourly=temperature_2m,precipitation,visibility,snowfall,wind_speed_10m,weather_code"
            "&current=temperature_2m,relative_humidity_2m,apparent_temperature,is_day,precipitation,rain,showers,snowfall"
            "&forecast_days=1"
        )
        try:
            response = requests.get(url, timeout=20)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logging.error(f"Error fetching weather for {city}: {e}")
            return None

    def run(self):
        while True:
            loop_start = time.time()
            now = datetime.now(timezone.utc)

            for city, coords in self.locations.items():
                data = self._fetch_city_weather(city, coords["lat"], coords["lon"])
                if data:
                    self.sink.write(data, {
                        "city": city,
                        "data_type": "hourly_weather",
                        "timestamp": now
                    })
                    logging.info(f"Staged weather data for {city} at {now}")

            elapsed = time.time() - loop_start
            time.sleep(max(0, POLL_INTERVAL - elapsed))

if __name__ == "__main__":
    storage = LocalFileSystemSink("./landing_zone/weather")
    producer = WeatherProducer(sink=storage)
    producer.run()