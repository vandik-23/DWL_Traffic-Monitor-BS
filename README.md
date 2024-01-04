# Project: Basel Traffic Monitor

## Description:
By addressing the challenge of anticipating pedestrian and cyclist traffic in Basel, we aim to create a data pipeline that seamlessly integrates public traffic information, holiday information and real-time weather data. This initiative will provide actionable insights for urban businesses to make informed decisions and optimize operations based on anticipated footfall and bicycle counts.

## Data Sources / Dynamic & Static APIs:

### 1. Traffic Numbers Bicycles and Pedestrians (Basel city):
 - [Data Source & Description](https://data.bs.ch/explore/dataset/100013/information/?sort=datetimefrom)
 - [Open Data API](https://data.bs.ch/api/explore/v2.1/catalog/datasets/100013/records?limit=20)

### 2. Weather Station Rosental Mitte Basel City:
 - [Data Source & Description](https://data.bs.ch/explore/dataset/100294/information/?sort=timestamp)
 - [Open Data API](https://data.bs.ch/api/explore/v2.1/catalog/datasets/100294/records?limit=20)

### 3. Public Holiday Days: Open Holuday API:
 - [Data Source & Description](https://www.openholidaysapi.org/de/)
 - [Open Data API](https://openholidaysapi.org/swagger/index.html)

### 4. Static Data Sources for Historical Data:
 - [Traffic Numbers Bicycles and Pedestrians (Basel city):](https://data-bs.ch/mobilitaet/converted_Velo_Fuss_Count.csv) Contains historic data points from 2004-2021
 - [Data source: Meteotest AG Weather API:](https://meteotest.ch/en/weather-api/klimadaten-1)
Meteonorm contains historic data points from 2020-2023: API for Measurements | Weather API


## Pipeline:

![Pipeline Architecture](https://github.com/vandik-23/DWLadies/blob/main/Pipeline_Architecture.png)

### 1. Static Data Ingestion:

| Nr. | Code                                                                                                                                           | Author                                                      |
|-------|---------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------|
| 1     | [Traffic]( Code Link ) | [Nina M.]( https://github.com/nmerryw )                      |
| 2     | [Weather API Connection]( https://github.com/vandik-23/DWLadies/blob/main/Weather_API_Connection.ipynb ) | [Natalie B.]( https://github.com/nbarnett19 )                      |
| 3     | [Open Holiday API Connection]( https://github.com/vandik-23/DWLadies/blob/main/Holiday_API_Connection.ipynb ) | [Andrea V.]( https://github.com/vandik-23 )                      |
|      | [AWS Lambda Layer]( https://github.com/vandik-23/DWLadies/blob/main/install_lambda_layer_python_3.9.txt ) | [Andrea V.]( https://github.com/vandik-23 )                      |

### 2. Dynamic Updates by AWS Lambda Functions:

| Nr. | Code                                                                                                                                           | Author                                                      |
|-------|---------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------|
| 1     | [Traffic Lambda Function]( Code Link ) | [Nina M.]( https://github.com/nmerryw )                      |
| 2     | [Weather Lambda Function]( https://github.com/vandik-23/DWLadies/blob/main/Weather_Lambda_Function.ipynb ) | [Natalie B.]( https://github.com/nbarnett19 )                      |
| 3     | [Holiday Lambda Function]( https://github.com/vandik-23/DWLadies/blob/main/Holiday_Lambda_Function.ipynb ) | [Andrea V.]( https://github.com/vandik-23 )                      |

### 3. Data Transformation:

| Nr. | Code                                                                                                                                           | Author                                                      |
|-------|---------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------|
| 1     | [Traffic Data]( Code Link ) | [Nina M.]( https://github.com/nmerryw )                      |
| 2     | [Weather Data Mage]( Code Link ) | [Natalie B.]( https://github.com/nbarnett19 )                      |
| 3     | [Holiday Data Transformation: Apache Airflow DAG]( https://github.com/vandik-23/DWLadies/blob/main/Holiday_DAG.py ) | [Andrea V.]( https://github.com/vandik-23 )                      |

## Output:

- [Traffic Monitor Tableau Public Dashboard](https://public.tableau.com/views/DWLadiesBaselTrafficMonitorFinal/BaselTrafficMonitor?:language=en-US&:display_count=n&:origin=viz_share_link)
