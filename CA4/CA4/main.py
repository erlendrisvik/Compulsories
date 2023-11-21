import streamsync as ss

# Set enviorment variables
from data_utils import *
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

def _get_df(table_name):

    (spark.read.format("org.apache.spark.sql.cassandra")
    .options(table=table_name, keyspace="compulsory")
    .load()
    .createOrReplaceTempView(table_name))

    df = spark.sql(f"select * from {table_name}").toPandas()
    df = df.sort_values(by=['week'])
    return df


initial_state = ss.init_state({
    "data": {
        "fish": _get_df(table_name = 'fish_data_full'),
        "lice": _get_df(table_name = 'lice_data_full')
    },
    "button_management":{
        "show_fish_years":False,
        "show_lice_years":False
    },
     "temporary_vars": {"selected_year": None
     },
      "messages": {"raiseWarning": False,
                 "raiseSuccess": False,
                 "raiseEmpty": False,
                 "raiseLoading": False},
                 "raiseError" : False,
})

# Set clickable cursor
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

    state['temporary_vars']['selected_year'] = None
    state['temporary_vars']['selected_year'] = payload

def write_fish_data(state):
    """Function to write fish data to state"""
    
    clean_all_messages(state)

    if not state['temporary_vars']['selected_year']:
        state['messages']['raiseEmpty'] = True
        state['messages']['raiseLoading'] = False
        return
    state['messages']['raiseLoading'] = True
    
    try:
        get_one_year_fish_data(int(state['selected_year']), get_access_token())
    except:
        state['messages']['raiseError'] = True
        state['messages']['raiseLoading'] = False
        return

    state['data']['fish'] = _get_df(table_name = 'fish_data_full')

    state['messages']['raiseLoading'] = False
    state['messages']['raiseSuccess'] = True
    state['messages']['selected_year'] = None

def clean_messages_not_loading(state):
    """Function to clean, but loading remains"""
    
    state['messages']['raiseSuccess'] = False
    state['messages']['raiseEmpty'] = False
    state['messages']['raiseWarning'] = False
    state['messages']['raiseError'] = False

def clean_all_messages(state):
    """Function to clean all messages"""

    state['messages']['raiseSuccess'] = False
    state['messages']['raiseEmpty'] = False
    state['messages']['raiseWarning'] = False
    state['messages']['raiseLoading'] = False
    state['messages']['raiseError'] = False
