import os
from pyspark.sql import SparkSession
import requests
import pandas as pd
import numpy as np
from cassandra.cluster import Cluster
from exceptions import *


# Set up request
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
