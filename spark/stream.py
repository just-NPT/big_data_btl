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
    .config("spark.es.nodes", "elasticsearch")\
    .config("spark.es.port", "9200")\
    .config("spark.es.nodes.wan.only", "false")\
    .config("es.index.auto.create", "true")\
    .enableHiveSupport()\
    .getOrCreate()

schema = StructType([
    StructField("review_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("product_id", StringType(), True)
])

spark.sparkContext.setLogLevel("WARN")
# es = Elasticsearch(hosts='http://elasticsearch:9200')
# # if not(es.indices.exists(index="realtime")):
# es.indices.create(index="realtime_stocks_ssi")

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "review") \
    .option("startingOffsets", "latest") \
    .load()

df.selectExpr("CAST(value AS STRING)").writeStream \
    .format("console") \
    .outputMode("append") \
    .start() \
    .awaitTermination()

