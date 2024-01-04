import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    date_trunc, col, split, decode, from_json, to_json
)
# from confluent_kafka import Consumer, KafkaError
# import pandas as pd
from pyspark.sql.types import *

spark = SparkSession.builder\
    .config("spak.app.name", "StreamStock")\
    .config("spark.master", "local[*]")\
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.2,org.elasticsearch:elasticsearch-spark-30_2.12:7.15.1")\
    .enableHiveSupport()\
    .getOrCreate()

# df = spark.read.parquet("hdfs://namenode:9000/test/test.parquet/")

df = spark.read.format('parquet')\
    .load("hdfs://namenode:9000/test/*.parquet")

df = spark.read.format('parquet')\
    .load("hdfs://namenode:9000/test/part-00000-cbf11df6-e47f-43cd-bc22-7388401f097d-c000.snappy.parquet")
print(df.head(10))

df.show(3)