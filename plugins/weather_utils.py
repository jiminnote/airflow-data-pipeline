import requests
from typing import Optional,Dict
from datetime import datetime

class WeatherAPI:
    BASE_URL = "https://api.openweathermap.org/data/2.5/weather"

    def __init__(self, api_key: str):
        self.api_key = api_key

    def get_weather(self, city:str) -> Optional[Dict]:

        try:
            response = requests.get(
                self.BASE_URL,
                params={
                    "q": city,
                    "appid": self.api_key,
                    "units": "metric",
                    "lang": "kr"
                },
                timeout=10
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            print(f"Error fetching weather data: {e}")
            return None

class WeatherValidator:
    def vallidate(data: Dict, city: str) -> tuple[bool, str]:
        if not data:
            return False, f"No data for {city}"
        required_keys = [ "main", "weather", "name"]
        for field in required_keys:
            if field not in data:
                return False, f"Missing field {field} in data for {city}"
        
        temp = data['main'].get('temp')
        if temp is None:
            return False, f"Missing temperature data for {city}"
        
        if not -50 <= temp <= 60:
            return False, f"Unrealistic temperature {temp}°C for {city}"
        
        humidity = data['main'].get('humidity')
        if humidity in None or not 0 <= humidity <= 100:
            return False, f"Invalid humidity {humidity}% for {city}"
        
        return True, "Data is valid"
    
class WeatherProcessor:
    @staticmethod
    def categorize_weather(weather_main: str) -> str:
        weather_main = weather_main.lower()

        if 'clear' in weather_main or 'sum' in weather_main:
            return 'Sunny'
        elif 'rain' in weather_main or 'drizzle' in weather_main:
            return 'Rainy'
        elif 'cloud' in weather_main:
            return 'Cloudy'
        elif 'snow' in weather_main:
            return 'Snowy'
        elif 'thunder' in weather_main or 'storm' in weather_main:
            return 'Stormy'
        else:
            return 'other'
        
    @staticmethod
    def transform(raw_data: Dict) -> Dict:

        processed_data = {
            "city": raw_data['name'],
            "country" : raw_data['sys']['country'],
            "temperature": raw_data['main']['temp'],
            "feels_like": raw_data['main']['feels_like'],
            "humidity": raw_data['main']['humidity'],
            "pressure": raw_data['main']['pressure'],
            "weather_main": raw_data['weather'][0]['main'],
            "weather_description": raw_data['weather'][0]['description'],
            "weather_category": WeatherProcessor.categorize_weather(raw_data['weather'][0]['main']),
            "wind_speed": raw_data['wind']['speed'],
            "timestamp": datetime.utcnow().isoformat() + 'Z'
        }
        return processed_data