from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    date_trunc, col, split, decode, from_json, to_json
)
# from elasticsearch import Elasticsearch

from pyspark.sql.types import *
# fix version 2.12:3.3.1  (3.3.1 is version spark)
spark = SparkSession.builder\
    .config("spak.app.name", "StreamStock")\
    .config("spark.master", "local[*]")\
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.2,org.elasticsearch:elasticsearch-spark-30_2.12:7.15.1")\
    .enableHiveSupport()\
    .getOrCreate()

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "review") \
    .option("startingOffsets", "latest") \
    .load()

