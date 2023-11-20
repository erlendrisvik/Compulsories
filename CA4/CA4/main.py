import streamsync as ss

# Set enviorment variables
from data_utils import get_access_token
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

table_name = 'fish_data_full'

(spark.read.format("org.apache.spark.sql.cassandra")
 .options(table=table_name, keyspace="compulsory")
 .load()
 .createOrReplaceTempView(table_name))

fish_data = spark.sql(f"select * from {table_name}").toPandas()
print(fish_data.head())


