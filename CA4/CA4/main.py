import streamsync as ss

# Set enviorment variables
from data_utils import *
import os
from pyspark.sql import SparkSession
import pandas as pd
import numpy as np
from cassandra.cluster import Cluster
import plotly.express as px

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

def list_fish_years(state):
    """Function to list all years in fish data"""

    years = state['data']['fish']['year'].unique()
    years.sort()
    state['fish_years'] = {str(i): int(years[i]) for i in range(len(years))}
    
    state['button_management']['ListFishYearsButtonClicked'] = not state['button_management']['ListFishYearsButtonClicked']

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
        state['messages']['raiseEmptyFieldWarning'] = True
        state['messages']['raiseLoading'] = False
        return
    
    state['messages']['raiseLoading'] = True
    
    try:
        get_one_year_fish_data(int(state['temporary_vars']['selected_year']), get_access_token())
    except InvalidYearError:
        state['messages']['raiseInvalidYearWarning'] = True
        state['messages']['raiseLoading'] = False
        return
    except DataExistsError:
        state['messages']['raiseDataExistWarning'] = True
        state['messages']['raiseLoading'] = False
        return
    except FetchDataError:
        state['messages']['raiseFetchDataError'] = True
        state['messages']['raiseLoading'] = False
        return
    except WritingToDatabaseError:
        state['messages']['raiseWriteDBError'] = True
        state['messages']['raiseLoading'] = False
        return
    
    state['data']['fish'] = _get_df(table_name = 'fish_data_full')

    state['messages']['raiseLoading'] = False
    state['messages']['raiseSuccess'] = True
    state['messages']['selected_year'] = None

def clean_messages_not_loading(state):
    """Function to clean, but loading remains"""
    # state['messages'].state
    state['messages']['raiseInvalidYearWarning'] = False
    state['messages']['raiseDataExistWarning'] = False
    state['messages']['raiseEmptyFieldWarning'] = False
    state['messages']['raiseFetchDataError'] = False
    state['messages']['raiseWriteDBError'] = False
    state['messages']['raiseSuccess'] = False

def clean_all_messages(state):
    """Function to clean all messages"""

    state['messages']['raiseInvalidYearWarning'] = False
    state['messages']['raiseDataExistWarning'] = False
    state['messages']['raiseEmptyFieldWarning'] = False
    state['messages']['raiseFetchDataError'] = False
    state['messages']['raiseWriteDBError'] = False
    state['messages']['raiseLoading'] = False
    state['messages']['raiseSuccess'] = False

def set_current_plot_year(state, payload):
    """Function to set current plot year"""

    pass

def _update_plotly_fish(state):
    fish_data = state['data']['fish']
    selected_num = state["plotly_settings"]["selected_num"]
    sizes = [10]*len(fish_data)

    if selected_num != -1:
        sizes[selected_num] = 20

    fig_fish = px.scatter_mapbox(
        fish_data,
        lat="lat",
        lon="lon",
        hover_name="name",
        hover_data=["municipality","lat","lon"],
        color_discrete_sequence=["darkgreen"],
        zoom=14,
        height=600,
        width=700,
    )
    overlay = fig_fish['data'][0]
    overlay['marker']['size'] = sizes
    fig_fish.update_layout(mapbox_style="open-street-map")
    fig_fish.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
    print(fig_fish)
#    state["plotly_settings"]["fish_map"] = fig_fish

#def handle_click(state, payload):
 #   fish_data = state["data"]["fish"]
  #  state["plotly_settings"]["selected_name"] = fish_data["name"].values[payload[0]["pointNumber"]]
   # state["plotly_settings"]["selected_num"] = payload[0]["pointNumber"]
    #_update_plotly_fish(state)


initial_state = ss.init_state({
    "data": {
        "fish": _get_df(table_name = 'fish_data_full'),
        "lice": _get_df(table_name = 'lice_data_full')
    },
    "button_management":{
        "ListFishYearsButtonClicked":False,
        "show_lice_years":False
    },
     "temporary_vars": {"selected_year": None
     },
      "messages": {"raiseInvalidYearWarning": False,
                 "raiseDataExistWarning" : False,
                 "raiseEmptyFieldWarning": False,
                 "raiseFetchDataError": False,
                 "raiseWriteDBError" : False,
                 "raiseLoading": False,
                 "raiseSuccess": False
    },
    "plotly_settings": {"selected_name": "Click to select",
                      "selected_num": -1,
                      "fish_map": None
    }
})

# Set clickable cursor
initial_state.import_stylesheet("theme", "/static/cursor.css")

_update_plotly_fish(initial_state)

