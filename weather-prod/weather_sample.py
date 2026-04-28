import pandas as pd
import requests

def get_detailed_weather(lat, lon, city_name):
    url = (
        f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}"
        "&hourly=temperature_2m,precipitation,wind_speed_10m,visibility,weather_code"
    )
    
    response = requests.get(url).json()
    df = pd.DataFrame(response['hourly'])
    df['time'] = pd.to_datetime(df['time'])
    
    print(f"\n--- {city_name} Detailed Data (First 5 Hours) ---")
    # This line prints the actual data rows
    print(df[['time', 'temperature_2m', 'precipitation', 'visibility']].head())
    
    return df

# Fetch and actually display the data
nyc_df = get_detailed_weather(40.71, -74.00, "New York City")
london_df = get_detailed_weather(51.50, -0.12, "London")