import datetime
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, concat, col
# from confluent_kafka import Consumer, KafkaError
# import pandas as pd
from pyspark.sql.types import *

spark = SparkSession.builder\
            .config("spark.app.name", "StockDataAnalyzer")\
            .config("spark.master", "spark://spark-master:7077")\
            .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.2.0")\
            .config("spark.cassandra.connection.host", "cassandra")\
            .config("spark.cassandra.auth.username", "cassandra")\
            .config("spark.cassandra.auth.password", "cassandra")\
            .enableHiveSupport()\
            .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

current_time = datetime.datetime.now()
hdfs_filename = "reviews/" +\
            str(current_time.year) + "/" +\
            str(current_time.month) + "/" +\
            str(current_time.day) + "/*"
            
df = spark.read.format('parquet')\
    .load(f"hdfs://namenode:9000/{hdfs_filename}")

df = df.drop('review')

df.write.format('org.apache.spark.sql.cassandra')\
        .mode('append')\
        .options(table='review_data', keyspace='review')\
        .save()

df.show(3)