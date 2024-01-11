# AirFlow Project
###### By: Nick Stanzione
###### Date: January 2024

### Summary
Spent a couple weeks learning Airflow to get familiar with some modern data engineer tools. Here is a practical sample project that culminates some of my learnings. This was an awesome project to finally get working. It took several days to figure out with lots of typical development set-backs. Figured there will be some interesting data that comes out of this set-up. Good understanding of the value of Airflow and how could be applied to data workflows.

#### Purpose: 
- Learn AirFlow (Hands-on-Keybaord)
- Learn simple AWS set-up and deployment

#### Overview of the technologies used:
- OpenWeather API
- Python
- AirFlow 
- AWS: S3, MWAA, VPC, SecretsManager, CloudWatch, IAM 

### Process
- Defined 3 python functions that calls OpenWeatherAPI
-- First Call gets the geolocation (latitude & longitude) of specified city (default to Seattle)
-- Second Call gets the weather data associated with the provided cooridnates
-- Third Call gets the air pollution data associated with provided coordinates
- The raw data is stored on an AWS S3 bucket via python function
- AirFlow is used to orchestrate this process to run hourly
- Several other AWS tools used to properly set-up Airflow in AWS with MWAA
- Python code continaing AirFlow DAG deployed to S3

### Python Programs
- raw_openweather.py

### Test Files
- test_weather_api.py

### Next Steps:
- Set-up GitHub Actions for deployment to S3 
- Follow-up analytics project after gathering more data

