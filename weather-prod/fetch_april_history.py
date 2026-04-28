import requests
import json
import os
from datetime import datetime

# Reuse your current coordinates
LOCATIONS = {
    "NYC": {"lat": 40.71, "lon": -74.00},
    "London": {"lat": 51.50, "lon": -0.12}
}

def fetch_history():
    # Use the same base path
    base_path = "./landing_zone/weather/history"
    os.makedirs(base_path, exist_ok=True)

    for city, coords in LOCATIONS.items():
        # past_days=20 gets us from April 1st to April 20th
        url = (
            f"https://api.open-meteo.com/v1/forecast?latitude={coords['lat']}&longitude={coords['lon']}"
            "&hourly=temperature_2m,precipitation,visibility,snowfall,wind_speed_10m,weather_code"
            "&past_days=20&forecast_days=0"
        )
        
        try:
            response = requests.get(url, timeout=20)
            response.raise_for_status()
            data = response.json()
            
            filename = f"{city}_April_History.json"
            filepath = os.path.join(base_path, filename)
            
            with open(filepath, "w") as f:
                json.dump(data, f, indent=4)
            print(f"Successfully saved April history for {city} to {filepath}")
            
        except Exception as e:
            print(f"Error fetching history for {city}: {e}")

if __name__ == "__main__":
    fetch_history()