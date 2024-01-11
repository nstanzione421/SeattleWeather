import requests
import json
from datetime import datetime
from datetime import timedelta

## Get Environment Variables -- Needed for test local run
import os
from dotenv import load_dotenv
load_dotenv()
api_key = os.environ['API_KEY']
path = os.environ['SAVE_PATH']


## Set Constants for Sample Script
city = 'Seattle'
state = 'Washington'
country = 'US'
limit = 1

## Set Constants for Airflow DAG
start_date = datetime(2024,1,7)
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

## Create functions to be used within the dag
def get_lat_lon(city, state, country):
    """
    Main Purpose: Call OpenWeatherAPI and get geolocation information
    Inputs: city, state, country
    Outputs: latitude, longitude coordinates
    Comments: See OpenWeather API documentation
    
    """
    base_url = 'http://api.openweathermap.org/geo/1.0/direct?'
    url = f'{base_url}q={city},{state},{country}&limit={limit}&appid={api_key}'
    response = requests.request('GET', url)
    response_js = response.json()[0]
    print(response_js)
    
    name = response_js['name']
    lat = response_js['lat']
    lon = response_js['lon']
    print(name, lat, lon)
    
    return lat, lon


def get_weather_data(lat, lon):
    """
    Main Purpose: Call OpenWeatherAPI and get weather information
    Inputs: latitude and longitude coordinates
    Outputs: weather data json object for location
    Comments: See OpenWeather API documentation
    
    """
    base_url = 'https://api.openweathermap.org/data/2.5/weather?'
    url = f'{base_url}lat={lat}&lon={lon}&units=Imperial&appid={api_key}'
    response = requests.request('GET', url)
    response_js = response.json()
    print(response_js)

    city = response_js['name']
    weather = response_js['weather'][0]['main']
    temp = response_js['main']['temp']
    unix_time = response_js['dt']
    dt = datetime.fromtimestamp(unix_time)
    timestamp = dt.strftime("%Y%m%d_%H%M%S")
    print(city, weather, temp, timestamp)

    return response_js, timestamp


def get_pollution_data(lat, lon):
    """
    Main Purpose: Call OpenWeatherAPI and get air pollution information
    Inputs: latitude and longitude coordinates
    Outputs: air pollution data json object for location
    Comments: See OpenWeather API documentation
    
    """
    base_url = 'http://api.openweathermap.org/data/2.5/air_pollution?'
    url = f'{base_url}lat={lat}&lon={lon}&appid={api_key}'
    response = requests.request('GET', url)
    response_js = response.json()
    print(response_js)

    coord = response_js['coord']
    aqi = response_js['list'][0]['main']['aqi']
    unix_time = response_js['list'][0]['dt']
    dt = datetime.fromtimestamp(unix_time)
    timestamp = dt.strftime("%Y%m%d_%H%M%S")
    print(coord, aqi, timestamp)

    return response_js, timestamp

def save_data(city, state, country):
    """
    Currently meant to be run locally according to path os variable.

    Main Purpose: Orchestrating OpenWeatherAPI functions and saving data
    Function that does the following:
        1) gets latitude & longitude
        2) gets weather data and saves as json file
        3) gets pollution data and saves as json file
    
    """
    lat, lon = get_lat_lon(city, state, country)
    
    weather_js, weather_dt = get_weather_data(lat, lon)
    with open(f"{path}/OpenWeatherData/weather_{city.lower()}_{weather_dt}.json", "w") as outfile:
        json.dump(weather_js, outfile)
   
    pollution_js, pollution_dt = get_pollution_data(lat, lon)
    with open(f"{path}/OpenWeatherData/pollution_{city.lower()}_{pollution_dt}.json", "w") as outfile:
        json.dump(pollution_js, outfile)
    
    print('Save Complete!')


## Run locally to test
save_data(city, state, country)
