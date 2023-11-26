import streamsync as ss
import plotly.express as px
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
    # quick fix to fix datatype.
    if table_name == 'fish_data_full':
        df['lat'] = df['lat'].astype(np.float64)
        df['lon'] = df['lon'].astype(np.float64)

    return df

def show_fish_years_button(state):
    """Function to list all years in fish data"""
    
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

    state['temporary_vars']['selected_fish_year'] = None
    state['temporary_vars']['selected_fish_year'] = payload

def write_fish_data(state):
    """Function to write fish data to state"""

    clean_all_messages(state)

    if not state['temporary_vars']['selected_fish_year']:
        state['messages']['raiseEmptyFieldWarning'] = True
        state['messages']['raiseLoading'] = False
        return
    
    state['messages']['raiseLoading'] = True
    
    try:
        get_one_year_fish_data(int(state['temporary_vars']['selected_fish_year']), get_access_token())
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
    state['messages']['selected_fish_year'] = None

def list_all_municipalities(state):
    """Function to list all municipalities"""

    municipalities = state['data']['fish']['municipality'].unique()
    municipalities.sort()
    state['variable_vars']['municipalities']= {str(i): municipalities[i] for i in range(len(municipalities))}

def store_selected_municipality(state, payload):
    """Function to store selected municipality"""

#    state['temporary_vars']['selected_municipality'] = None
    state['temporary_vars']['selected_municipality'] = payload

def store_selected_lice_locality(state, payload):
    """Function to store selected locality"""

#    state['temporary_vars']['selected_locality'] = None
    state['temporary_vars']['selected_locality'] = payload

def store_selected_lice_year(state, payload):
    state['temporary_vars']['selected_lice_year'] = None
    state['temporary_vars']['selected_lice_year'] = payload

def write_lice_data(state):
    clean_all_messages()   

    if not state['temporary_vars']['selected_lice_year']:
        state['messages']['raiseEmptyFieldWarning'] = True
        state['messages']['raiseLoading'] = False
        return
    
    if not state['temporary_vars']['selected_locality']:
        state['messages']['raiseEmptyFieldWarning'] = True
        state['messages']['raiseLoading'] = False
        return
    
    state['messages']['raiseLoading'] = True

    try:
        get_one_year_lice_data(int(state['temporary_vars']['selected_locality'] , int(state['temporary_vars']['selected_lice_year'])), get_access_token())
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
    
    state['data']['lice'] = _get_df(table_name = 'lice_data_full')

    state['messages']['raiseLoading'] = False
    state['messages']['raiseSuccess'] = True
    state['messages']['selected_lice_year'] = None


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

def _list_available_fish_years(state):
    years = state['data']['fish']['year'].unique()
    years = sorted(years, reverse=True)
    state['variable_vars']['available_fish_years'] = {str(i): int(years[i]) for i in range(len(years))}

def set_current_map_plot_year(state, payload):
    """Function to set current plot year"""

    state["plotly_settings_fish"]["selected_fish_year_plotly"] = state["variable_vars"]["available_fish_years"][payload]
    set_subsetted_fish_data(state)

def _setup_fish_map(state):
    fish_data = state["plotly_settings_fish"]["subsetted_fish_data"].copy()
    fig_fish = px.scatter_mapbox(
    fish_data,
    lat="lat",
    lon="lon",
    hover_name="name",
    hover_data=["localityno","lat","lon"],
    color_discrete_sequence=["darkgreen"],
    zoom=3,
    height=600,
    width=700,
)
    #fig_fish.layout.updatemenus[0].buttons[0].args[1]['frame']['duration'] = 30
    #fig_fish.layout.updatemenus[0].buttons[0].args[1]['transition']['duration'] = 5
    fig_fish.update_geos(projection_type="equirectangular", visible=True, resolution=50)


    sizes = [10]*len(fish_data)
   
    overlay = fig_fish['data'][0]
    overlay['marker']['size'] = sizes

    fig_fish.update_layout(mapbox_style="open-street-map")
    fig_fish.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
    state['plotly_settings_fish']['sizes'] = sizes
    state["plotly_settings_fish"]["fish_map"] = fig_fish

def _update_fish_map(state, last_clicked):
    fish_data = state["plotly_settings_fish"]["subsetted_fish_data"]
    selected_num = state["plotly_settings_fish"]["selected_num"]
    fig_fish = state["plotly_settings_fish"]["fish_map"]
    lat = fish_data.loc[selected_num, 'lat']
    lon = fish_data.loc[selected_num, 'lon']

    sizes = state["plotly_settings_fish"]["sizes"]

    if selected_num != -1:
        sizes[last_clicked] = 10
        sizes[selected_num] = 20

    overlay = fig_fish['data'][0]
    overlay['marker']['size'] = sizes

    fig_fish.update_layout(mapbox=dict(center=dict(lat=lat,
                                            lon=lon)))
    fig_fish['layout']['mapbox']['zoom'] = 9
    state["plotly_settings_fish"]["fish_map"] = fig_fish

def set_subsetted_fish_data(state):
    fish_data = state["data"]["fish"].copy()
    fish_data = fish_data[fish_data["year"] == state["plotly_settings_fish"]["selected_fish_year_plotly"]].reset_index(drop=True)
    state["plotly_settings_fish"]["subsetted_fish_data"] = fish_data
    

def handle_fish_map_click(state, payload):
    last_clicked = state["plotly_settings_fish"]["selected_num"]
    fish_data = state["plotly_settings_fish"]["subsetted_fish_data"].copy()
    state["plotly_settings_fish"]["selected_name"] = fish_data["name"].values[payload[0]["pointNumber"]]
    state["plotly_settings_fish"]["selected_num"] = payload[0]["pointNumber"]
    state["temporary_vars"]["current_selected_fish_locality"] = fish_data.loc[payload[0]["pointNumber"]]
    print(len(fish_data))
    _update_fish_map(state, last_clicked)

def setup_proportion_pd_fish_pie(state):
    top_10 = (state["plotly_settings_fish"]["subsetted_fish_data"]
              .groupby("municipality")["haspd"]
              .value_counts(normalize=True)
              .unstack(fill_value=0)
              .sort_values(by = True, ascending = False)
              .head(10)
              .reset_index())
    
    pie = px.pie(top_10, values=True, names='municipality', title='Proportion of PD within each locality')
    state["plotly_settings_fish"]["proportion_pd_fish_pie"] = pie

def _setup_fish_histogram(state):
    ignored_fish_cols = ['lat', 'lon', 'localityweekid', 'localityno', 'municipality', 'municipalityno', 'name', 'week', 'year']

    fish_data = state["plotly_settings_fish"]["subsetted_fish_data"].drop(columns=ignored_fish_cols)
    state["plotly_settings_fish"]["subsetted_histogram_fish_data"] = fish_data
    columns_sorted = fish_data.columns.sort_values().tolist()
    print(columns_sorted)
    state["constant_vars"]["fish_cols_histogram"] = dict(enumerate(columns_sorted))

    default_column = "haspd"
    fig_hist = px.histogram(fish_data, x = default_column)
    state["plotly_settings_fish"]["fish_histogram"] = fig_hist

def select_histogram_fish_col(state, payload):
    state["temporary_vars"]["selected_histogram_col"] = state["constant_vars"]["fish_cols_histogram"][payload]
    update_fish_histogram(state)

def update_fish_histogram(state):
    fish_data = state["plotly_settings_fish"]["subsetted_histogram_fish_data"]
    column = state["temporary_vars"]["selected_histogram_col"]
    fig_hist = px.histogram(fish_data, x = column)
    state["plotly_settings_fish"]["fish_histogram"] = fig_hist

initial_state = ss.init_state({
    "data": {
        "fish": _get_df(table_name = 'fish_data_full'),
        "lice": _get_df(table_name = 'lice_data_full')
    },
    "button_management":{
        "ListFishYearsButtonClicked":False,
        "show_lice_years":False
    },
     "temporary_vars": {"selected_fish_year": None,
                        "selected_lice_year": None,
                        "selected_municipality": None,
                        "selected_locality": None,
                        "selected_histogram_col": None,
                        "current_selected_fish_locality": None          
     },
     "variable_vars": {"municipalities": None,
                       "available_fish_years": None
     },
     "constant_vars":{"fish_cols_histogram": None
    },
      "messages": {"raiseInvalidYearWarning": False,
                 "raiseDataExistWarning" : False,
                 "raiseEmptyFieldWarning": False,
                 "raiseFetchDataError": False,
                 "raiseWriteDBError" : False,
                 "raiseLoading": False,
                 "raiseSuccess": False
    },
    "plotly_settings_fish": {"selected_name": "Click to select",
                      "selected_num": -1,
                      "selected_fish_year_plotly": 2015,
                      "fish_map": None,
                      "fish_histogram": None,
                      "proportion_pd_fish_pie": None,
                      "subsetted_fish_data": None,
                      "subsetted_histogram_fish_data": None,
                      "sizes": None
    }
})

# Set clickable cursor
initial_state.import_stylesheet("theme", "/static/cursor.css")

set_subsetted_fish_data(initial_state)
_setup_fish_map(initial_state)
_setup_fish_histogram(initial_state)
setup_proportion_pd_fish_pie(initial_state)
#_update_plotly_fish(initial_state)
_list_available_fish_years(initial_state)
