import requests
import json
from datetime import datetime
from datetime import timedelta
import boto3
import airflow
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable

# ## Get Environment Variables
secrets = Variable.get("ow_secrets")
secrets_js = json.loads(secrets)
api_key = secrets_js.get("api_key")
bucket = secrets_js.get("bucket")

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
    "retry_delay": timedelta(minutes=2)
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
    Built for AWS and S3 storage to personal bucket

    Main Purpose: Orchestrating OpenWeatherAPI functions and saving data
    Function that does the following:
        1) gets latitude & longitude
        2) gets weather data and saves as json file
        3) gets pollution data and saves as json file
    
    """
    lat, lon = get_lat_lon(city, state, country)
    
    weather_js, weather_dt = get_weather_data(lat, lon)
    weather_file = f"weather_{city.lower()}_{weather_dt}.json"
    with open(weather_file, "w") as outfile:
        json.dump(weather_js, outfile)

    pollution_js, pollution_dt = get_pollution_data(lat, lon)       
    pollution_file = f"pollution_{city.lower()}_{pollution_dt}.json"
    with open(pollution_file, "w") as outfile:
        json.dump(pollution_js, outfile)
    
    s3 = boto3.client('s3')
    s3.upload_file(weather_file, bucket, f"raw-data/weather/{weather_file}")
    s3.upload_file(pollution_file, bucket, f"raw-data/air-pollution/{pollution_file}")

    print('Save Complete!')


## Setting up Airflow DAG
with DAG(
    "raw_openweather",
    default_args=default_args,
    description="first airflow dag",
    schedule_interval=timedelta(minutes=60),
    start_date=start_date,
    catchup=False,
    tags=["dag"]
) as dag:

    ## Create task for Airflow referencing functions above using PythonOperator
    extract_openweather_data = PythonOperator(
        task_id='extract_weather',
        python_callable=save_data,
        op_kwargs={
            'city':city,
            'state':state,
            'country':country
        },
        dag=dag
    )

    ## Empty Operator to always know when DAG is finished
    ready = EmptyOperator(task_id='ready')

    ## DAG ordering, extract_openweather_data task first, then run ready task
    extract_openweather_data >> ready