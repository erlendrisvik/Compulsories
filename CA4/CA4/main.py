import streamsync as ss

# Set enviorment variables
from data_utils import get_access_token, get_one_year_fish_data, get_one_year_lice_data
import os
from pyspark.sql import SparkSession
import pandas as pd
import numpy as np
from cassandra.cluster import Cluster

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

_access_token = get_access_token()


def _get_df(table_name):

    (spark.read.format("org.apache.spark.sql.cassandra")
    .options(table=table_name, keyspace="compulsory")
    .load()
    .createOrReplaceTempView(table_name))

    df = spark.sql(f"select * from {table_name}").toPandas()
    df = df.sort_values(by=['week'])
    return df


initial_state = ss.init_state({
    "my_app": {
        "title": "Barentswatch"
    },
    "_my_private_element": 1337,
    "message": None,
    "counter": 26,
    "selected":"Click to select",
    "selected_num":-1,
    "data": {
        "fish": _get_df(table_name = 'fish_data_full'),
        "lice": _get_df(table_name = 'lice_data_full')
    },
    "button_management":{
        "show_fish_years":False,
        "show_lice_years":False
    },
    "selected_year": None,
})

initial_state.import_stylesheet("theme", "/static/cursor.css")

def list_fish_years(state):
    """Function to list all years in fish data"""

    years = state['data']['fish']['year'].unique()
    state['fish_years'] = {str(i): int(years[i]) for i in range(len(years))}
    
    state['button_management']['show_fish_years'] = not state['button_management']['show_fish_years']

def list_lice_years_and_locality(state):
    """Function to list all years and localities in lice data"""

    year_locality_dict = {str(i): {'year': int(state['data']['lice'][['year', 'localityno']].drop_duplicates().values.tolist()[i][0]),
                                 'localityno': int(state['data']['lice'][['year', 'localityno']].drop_duplicates().values.tolist()[i][1])}
                        for i in range(len(state['data']['lice'][['year', 'localityno']].drop_duplicates().values.tolist()))}
    
    state['lice_years_and_locality'] = year_locality_dict
    state['button_management']['show_lice_years'] = not state['button_management']['show_lice_years']

def store_selected_fish_year(state, payload):
    """Function to store selected fish year"""

    state['selected_year'] = payload

def write_fish_data(state):
    """Function to write fish data to state"""
    
    if not state['selected_year']:
        state['raiseEmpty'] = True
        return
    
    state['raiseEmpty'] = False

    