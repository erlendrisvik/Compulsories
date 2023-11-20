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
        "lice": _get_df(table_name = 'lice_data_full'),
    }
})

def fetch_new_fish_data(state, payload, access_token):
    get_one_year_fish_data(payload, access_token)

