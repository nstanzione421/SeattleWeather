import requests
import json
import os
import boto3
from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

## Get Environment Variables -- Needed for test local run
import os
from dotenv import load_dotenv
load_dotenv()
api_key = os.environ['OW_API_KEY']
city = 'Seattle'
state = 'Washington'
country = 'US'
limit = 1
# path = r'/app'

start_date = datetime(2024,1,6)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

## Set-up access to AWS S3 Bucket
aws_access_key = os.environ['AWS_ACCESS_KEY']
aws_secret_key = os.environ['AWS_SECRET_KEY']
bucket_name = 'open-weather-stanz'
s3_client = boto3.client(
    's3',
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key
)

def upload_to_s3(js, path_name):
    """
    Main Purpose: Function to upload json object to s3 bucket path
    Inputs: json object, s3 path
    
    Uses similar approach to a HTTP Put Request. Note we convert json object to a string.
    
    """
    response = s3_client.put_object(
        Bucket=bucket_name, 
        Key=path_name,
        Body=json.dumps(js)
    )
    print(response)

    
def get_lat_lon(city, state, country, ti):
    """
    Main Purpose: Call OpenWeatherAPI and get geolocation information
    Inputs: city, state, country
    Outputs: latitude, longitude coordinates
    Comments: See OpenWeather API documentation
    
    Airflow: Uses Xcom. Pushes results into task instance to be pulled by other tasks within DAG.
    
    """
    base_url = 'http://api.openweathermap.org/geo/1.0/direct?'
    url = f'{base_url}q={city},{state},{country}&limit={limit}&appid={api_key}'
    response = requests.request('GET', url)
    response_js = response.json()[0]
    name = response_js['name']
    lat = response_js['lat']
    lon = response_js['lon']
    print(name, lat, lon)

    ## Push data for other tasks to reference.
    ti.xcom_push(key='city', value=name)
    ti.xcom_push(key='lat', value=lat)
    ti.xcom_push(key='lon', value=lon)
    
    return name, lat, lon


def get_weather_data(ti):
    """
    Main Purpose: Call OpenWeatherAPI and get weather information
    Inputs: latitude and longitude coordinates
    Outputs: weather data json object for location
    Comments: See OpenWeather API documentation
    
    Airflow: Uses Xcom. Pulls results from geolocation task to use within this task.

    """
    ## Grab data from the geo_location task. Inputs into this function.
    city = ti.xcom_pull(key='city', task_ids='geo_location')
    lat = ti.xcom_pull(key='lat', task_ids='geo_location')
    lon = ti.xcom_pull(key='lon', task_ids='geo_location')

    ## Call API to get lat/lon coordinates
    base_url = 'https://api.openweathermap.org/data/2.5/weather?'
    url = f'{base_url}lat={lat}&lon={lon}&units=Imperial&appid={api_key}'
    response = requests.request('GET', url)
    response_js = response.json()
    print(response_js)

    ## Call API to get weather data
    weather = response_js['weather'][0]['main']
    temp = response_js['main']['temp']
    unix_time = response_js['dt']
    dt = datetime.fromtimestamp(unix_time)
    timestamp = dt.strftime("%Y%m%d_%H%M%S")
    print(city, weather, temp, timestamp)

    file_name = f'weather_{city.lower()}_{timestamp}.json'
    upload_to_s3(response_js, f'raw-data/weather/{file_name}')
        
    print('Weather Save Complete!')



def get_pollution_data(ti):
    """
    Main Purpose: Call OpenWeatherAPI and get air pollution information
    Inputs: latitude and longitude coordinates
    Outputs: air pollution data json object for location
    Comments: See OpenWeather API documentation

    Airflow: Uses Xcom. Pulls results from geolocation task to use within this task.
    
    """
    ## Grab data from the geo_location task. Inputs into this function.
    city = ti.xcom_pull(key='city', task_ids='geo_location')
    lat = ti.xcom_pull(key='lat', task_ids='geo_location')
    lon = ti.xcom_pull(key='lon', task_ids='geo_location')

    ## Call API to get pollution data
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

    file_name = f'pollution_{city.lower()}_{timestamp}.json'
    upload_to_s3(response_js, f'raw-data/pollution/{file_name}')
    
    print('Pollution Save Complete!')


with DAG(
    "raw_openweather",
    default_args=default_args,
    description="first airflow dag",
    schedule_interval=timedelta(minutes=10),
    start_date=start_date,
    catchup=False,
    tags=["dag"]
) as dag:

    ## Create task for Airflow referencing function above using PythonOperator    
    geo_location = PythonOperator(
        task_id='geo_location',
        python_callable=get_lat_lon,
        op_kwargs={
            'city':city,
            'state':state,
            'country':country
        },
        provide_context=True ## Needed for Xcom
    )

    ## Create task for Airflow referencing function above using PythonOperator
    extract_weather = PythonOperator(
        task_id='extract_weather',
        python_callable=get_weather_data,
        provide_context=True ## Needed for Xcom
    )

    ## Create task for Airflow referencing function above using PythonOperator
    extract_pollution = PythonOperator(
        task_id='extract_pollution',
        python_callable=get_pollution_data,
        provide_context=True ## Needed for Xcom
    )

    ## Empty Operator to always know when DAG is finished
    ready = EmptyOperator(
        task_id='ready'
    )

    ## DAG ordering. 1 > [2,3] > 4. Lat lon coordinates first. Weather and pollution can sun same time. Ready runs after all others. 
    geo_location >> [extract_weather, extract_pollution] >> ready
