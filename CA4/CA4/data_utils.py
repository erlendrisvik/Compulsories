import os
from pyspark.sql import SparkSession
import requests
import pandas as pd
import numpy as np
from cassandra.cluster import Cluster
from exceptions import *
import ast

def get_access_token():
    """Function to get access token from Barentswatch API

    Returns:
        str: Access token
    """
    url = "https://id.barentswatch.no/connect/token"
    # Read secret key from file
    secret_key = open(r'..\..\..\IND320\No_sync\fish_api', 'r').read()

    # Set up request to get access token
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {
        "client_id": "erlend.risvik@gmail.com:fishclient",
        "scope": "api",
        "client_secret": secret_key,
        "grant_type": "client_credentials"
    }

    response = requests.post(url, headers=headers, data=data)
    return response.json()['access_token']

def convert_to_spark(df):
    """Function to convert pandas dataframe to spark dataframe

    Parameters:
    -----------
    df : pandas dataframe
        Dataframe to convert

    Returns:
    --------
    spark dataframe
    """
    return spark.createDataFrame(df)

def write_to_cassandra(df, table_name):
    """Function to write data to cassandra database

    Parameters:
    -----------
    df : pandas dataframe
        Dataframe to write
    table_name : str
        Name of table to write to
    """
    
    df_spark = convert_to_spark(df)
    (df_spark.write
     .format("org.apache.spark.sql.cassandra")
     .options(table=table_name, keyspace="compulsory")
     .mode("append")
     .save())

def check_exist_fish(year):
    """Function to check if data exists in database.'
    Parameters:
    -----------
    year : int
        Year of data

    Returns:
    --------
    bool: True if data exists, False if not
    """

    (spark.read.format("org.apache.spark.sql.cassandra")
    .options(table = 'fish_data_full', keyspace="compulsory")
    .load()
    .createOrReplaceTempView('fish_data_full'))
    
    check = spark.sql(f"SELECT count(*) FROM fish_data_full WHERE year = {year}")   
    return check.collect()[0][0] >= 1 

def check_exist_lice(locality, year):
    """Function to check if data exists in database.'
    Parameters:
    -----------
    locality : int
        Locality number
    year : int
        Year of data

    Returns:
    --------
    bool: True if data exists, False if not
    """

    (spark.read.format("org.apache.spark.sql.cassandra")
    .options(table = 'lice_data_full', keyspace="compulsory")
    .load()
    .createOrReplaceTempView('lice_data_full'))
    
    check = spark.sql(f"SELECT count(*) FROM lice_data_full WHERE year = {year} AND localityno = {locality}")   
    return check.collect()[0][0] >= 1 

def check_exist_weather(locality, year):
    """Function to check if data exists in database.'
    
    Parameters:
    -----------
    locality : int
        Locality number
    year : int
        Year of data

    Returns:
    --------
    bool: True if data exists, False if not
    """

    (spark.read.format("org.apache.spark.sql.cassandra")
    .options(table = 'weekly_weather_data', keyspace="compulsory")
    .load()
    .createOrReplaceTempView('weekly_weather_data'))
    
    check = spark.sql(f"SELECT count(*) FROM weekly_weather_data WHERE year = {year} AND localityno = {locality}")   
    return check.collect()[0][0] >= 1 

def get_one_week_fish_data(year, week, access_token):
    """Function to get fish data from Barentswatch API.
    
    Parameters:
    -----------
    year : int
        Year of data
    week : int
        Week of data
    access_token : str
        Access token from Barentswatch API
    Returns:
    --------
    json: json object with data
    """

    # Set url to correct API address
    url = f"https://www.barentswatch.no/bwapi/v1/geodata/fishhealth/locality/{year}/{week}"

    headers = {
        "Authorization": "Bearer "+ access_token}

    df = requests.get(url, headers = headers).json()
    return df

def get_one_year_fish_data(year, access_token):
    """Function to get all fish data from Barentswatch API limited to one year.

    Parameters:
    -----------
    access_token : str
        Access token from Barentswatch API
    Returns:
    --------
    df: pandas dataframe with data
    """
    # check if year is between 2010 and 2023
    if year < 2010 or year > 2023:
        raise InvalidYearError("Year invalid")

    if check_exist_fish(year):
        raise DataExistsError("Data exists")

    # Set list of weeks (1-52).
    weeks = np.arange(1, 53)
    df = pd.DataFrame()

    try:
        for week in weeks:
            data = get_one_week_fish_data(year = year, week = week, access_token = access_token)["localities"]
            data = pd.DataFrame(data)
            data["year"] = year
            data["week"] = week
            df = pd.concat([df, data], ignore_index=True)

        df.columns = df.columns.str.lower()
    except:
        raise FetchDataError("Error fetching data")

    try:
        write_to_cassandra(df = df, table_name = "fish_data_full")
    except:
        raise WritingToDatabaseError("Error writing to database") 

def get_one_week_lice_data(localty, year, week, access_token):
    """Function to get lice count data from Barentswatch API.

    Parameters:
    -----------
    localty : int
        Localty number
    year : int
        Year of data
    week : int
        Week of data
    access_token : str
        Access token from Barentswatch API

    Returns:
    --------
    json: json object with data 
    """

    # Set url to correct API address
    url = f'https://www.barentswatch.no/bwapi/v1/geodata/fishhealth/locality/{localty}/{year}/{week}'
    headers = {
        "Authorization": "Bearer "+ access_token}
    
    df = requests.get(url, headers=headers).json()
    return df

def get_one_year_lice_data(locality, year, access_token):
    """
    Function to get all lice count data from Barentswatch API limited to one year.

    Parameters:
    -----------
    localty : int
        Localty number
    year : int
        Year of data
    access_token : str
        Access token from Barentswatch API
    Returns:
    --------
    df: pandas dataframe with data
    """

    #if year < 2010 or year > 2023:
     #   raise InvalidYearError("Year invalid")

    if check_exist_lice(locality=locality, year = year):
        raise DataExistsError("Data exists")

    # Set list of weeks (1-52).
    weeks = np.arange(1, 53)
    df = pd.DataFrame()

    try:
        for week in weeks:
            data = get_one_week_lice_data(localty = locality, year = year, week = week, access_token = access_token)["localityWeek"]
            for key, value in data.items():
                # Set to list to make it compatible to convert to pandas dataframe
                data[key] = [value]
            # Dropping columns that contain purely None and nested dictionaries
            data = pd.DataFrame(data).drop(columns = ["bathTreatments", "cleanerFish", "inFeedTreatments", \
                                                    "mechanicalRemoval", "timeSinceLastChitinSynthesisInhibitorTreatment"]) 
            data["year"] = year
            data["week"] = week
            df = pd.concat([df, data], ignore_index=True)
        # Lowercase column names
        df.columns = df.columns.str.lower()
    except:
        raise FetchDataError("Error fetching data")
    
    if True in df.isnull().all().to_list():
        raise NoDataError("No data for this locality")
    
    try:
        write_to_cassandra(df = df, table_name = "lice_data_full")
    except:
        raise WritingToDatabaseError("Error writing to database") 


def get_cords(df, localityno):
    """Function to get coordinates from dataframe

    Parameters:
    -----------
    df : pandas dataframe
        Dataframe to get coordinates from
    localityno : int
        Locality number to get coordinates from

    Returns:
    --------
    list: list of tuples with coordinates
    """
    # there are multiple rows with the same localityno, so we need to get the first one
    subset = df[df["localityno"] == localityno].iloc[0:1]

    return float(subset["lat"]), float(subset["lon"])

def get_nearest_stations(lat, lon):
    """Function to get nearest weather station from frost.met.no

    Parameters:
    -----------
    lat : float
        Latitude
    lon : float
        Longitude

    Returns:
    --------
    json: json object with data
    """
    # Set up parameters

    endpoint = 'https://frost.met.no/sources/v0.jsonld'
    parameters = {
    "geometry" : f"nearest(POINT({lon} {lat}))",
    "nearestmaxcount": 20,
    }

    # Issue an HTTP GET request
    r = requests.get(endpoint, parameters, auth=(SECRET_ID,''))
    # Extract JSON data
    json = r.json()

    # Check if the request worked, print out any errors
    if r.status_code == 200:
        data = json['data']
        # extract the list of source ids and distance as a tuple
        data = [(d['id'], d['distance']) for d in data]
        return data
    else:
        raise FetchDataError(f"Request failed with status code {r.status_code}")
    
def get_daily_data(df, localityno, year):
    """Function to get daily weather data from frost.met.no

    Parameters:
    -----------
    df : pandas dataframe
        Dataframe to get coordinates from
    localityno : int
        Locality number
    year : int
        Year of data

    Returns:
    --------
    df3: pandas dataframe with data
    """ 
    try:
        lat, lon = get_cords(df = df, localityno = localityno)
        stations = get_nearest_stations(lat, lon)
    except:
        raise FetchDataError("Error fetching data")
    
    ids = [d[0] for d in stations]
    distances = [d[1] for d in stations]
   
    endpoint = 'https://frost.met.no/observations/v0.jsonld'

    for idx, id in enumerate(ids):
        parameters = {
            'sources': id,
            'elements': 'sum(precipitation_amount P1D), mean(air_temperature P1D), mean(wind_speed P1D), mean(relative_humidity P1D)',
            'referencetime': f"{year}-01-01/{year}-12-31",
            'levels' : 'default',
            'timeoffsets': 'default'
        }

    # Issue an HTTP GET request
        r = requests.get(endpoint, parameters, auth=(SECRET_ID,''))
        # Extract JSON data
        json = r.json()

        df = pd.DataFrame()
        try: 
            data = json['data']
            for i in range(len(data)):
                row = pd.DataFrame(data[i]['observations'])
                row['referenceTime'] = data[i]['referenceTime']
                row['sourceId'] = data[i]['sourceId']
                df = pd.concat([df, row], ignore_index=True)

            df = df.reset_index(drop=True)

            columns = ['sourceId','referenceTime','elementId','value','unit','timeOffset']
            df2 = df[columns].copy()
            df2['referenceTime'] = pd.to_datetime(df2['referenceTime']).dt.strftime('%Y-%m-%d')
            
            df3 = df2.pivot(index='referenceTime', columns='elementId', values='value').reset_index()      
            df3.columns = ['date', 'temperature', 'humidity', 'wind_speed', 'precipitation']
        except:
            if idx == len(ids)-1 or distances[idx]>50:
                raise NoDataError("No data available")
            continue
        
        # add the distance as a column
        df3['distance'] = distances[idx]
        df3['localityno'] = localityno
        return df3
    
def convert_to_weekly_data(weather_data):
    weather_data['date'] = pd.to_datetime(weather_data['date'])
    weather_data['week'] = weather_data['date'].dt.isocalendar().week
    weather_data['year'] = weather_data['date'].dt.isocalendar().year

    # create the weekly_weather_data_mean DataFrame where we aggregate by weekly means
    weekly_weather_data_mean = pd.DataFrame()
    weekly_weather_data_mean['week'] = weather_data['week']
    weekly_weather_data_mean['humidity'] = weather_data['humidity']
    weekly_weather_data_mean['temperature'] = weather_data['temperature']
    weekly_weather_data_mean['wind_speed'] = weather_data['wind_speed']
    weekly_weather_data_mean = weekly_weather_data_mean.groupby('week').mean()

    # same for precipitation, but we use weekly sum
    weekly_weather_data_sum = pd.DataFrame()
    weekly_weather_data_sum['week'] = weather_data['week']
    weekly_weather_data_sum['precipitation'] = weather_data['precipitation']
    weekly_weather_data_sum = weekly_weather_data_sum.groupby('week').sum()

    # merging the two dataframes
    weekly_weather_data = pd.merge(weekly_weather_data_mean, weekly_weather_data_sum, left_index=True, right_index=True)

    # add the year, week and localityno columns
    weekly_weather_data['year'] = weather_data['year'][0]
    weekly_weather_data['week'] = weekly_weather_data.index
    weekly_weather_data['localityno'] = weather_data['localityno'][0]
    weekly_weather_data = weekly_weather_data.reset_index(drop=True)

    # create a id column that is the concatenation of year_week_localityno
    weekly_weather_data['id'] = weekly_weather_data['year'].astype(str) + '_' + weekly_weather_data['week'].astype(str) + '_' + weekly_weather_data['localityno'].astype(str)

    return weekly_weather_data

def get_one_year_weather_data(df, locality, year):
    """Function to get all weather data from frost.met.no limited to one year.

    Parameters:
    -----------
    localty : int
        Localty number
    year : int
        Year of data
    access_token : str
        Access token from Barentswatch API
    Returns:
    --------
    df: pandas dataframe with data
    """
    if check_exist_weather(locality=locality, year = year):
        raise DataExistsError("Data exists")

    try:
        df = get_daily_data(df = df, localityno = locality, year = year)
    except (FetchDataError, NoDataError):
        raise NoDataError
    
    try: 
        weekly_data = convert_to_weekly_data(df)
    except:
        raise NoDataError("No data available")
    try:
        write_to_cassandra(df = weekly_data, table_name = "weekly_weather_data")
    except:
        raise WritingToDatabaseError("Error writing to database")

def clean_table(table_name):
    """Function to clean table in cassandra database

    Parameters:
    -----------
    table_name : str
        Name of table to clean
    """
    session.execute(f"TRUNCATE {table_name}")

# Set pyspark env
os.environ["PYSPARK_PYTHON"] = "python"

spark = SparkSession.builder.appName('SparkCassandraApp').\
    config('spark.jars.packages', 'com.datastax.spark:spark-cassandra-connector_2.12:3.4.1').\
    config('spark.cassandra.connection.host', 'localhost').\
    config('spark.sql.extensions', 'com.datastax.spark.connector.CassandraSparkExtensions').\
    config('spark.sql.catalog.mycatalog', 'com.datastax.spark.connector.datasource.CassandraCatalog').\
    config('spark.cassandra.connection.port', '9042').getOrCreate()


cluster = Cluster(['localhost'], port=9042)
session = cluster.connect()
session.set_keyspace('compulsory')
#access_token = get_access_token()

SECRET_INFO = open("../../NO_SYNC/weather_api", 'r').read().replace('\n', '')
SECRET_ID = ast.literal_eval(SECRET_INFO)["client_id"]