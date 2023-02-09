from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    date_trunc, col, split, decode
)
from elasticsearch import Elasticsearch
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

spark.sparkContext.setLogLevel("WARN")
es = Elasticsearch(hosts='http://elasticsearch:9200')
# es.indices.create(index="stocks")
df = spark.readStream.format('kafka')\
    .option('kafka.bootstrap.servers', 'localhost:19092,localhost:29092,localhost:39092')\
    .option('subscribe', 'realtimeStockData')\
    .load()
df.printSchema()
df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
# df.writeStream.start().awaitTermination()
# df.show(5)

# print(df.value)
df = df.select(decode(col('value'), 'UTF-8').alias('value'),
            col('timestamp').alias('recorded_time'))

df = df.withColumn('symbol', (split(col('value'), ',').getItem(0)))
df = df.withColumn('volume', (split(col('value'), ',').getItem(1)))
df = df.withColumn('cp', (split(col('value'), ',').getItem(2)))
df = df.withColumn('rcp', (split(col('value'), ',').getItem(3)))
df = df.withColumn('a', (split(col('value'), ',').getItem(4)))
df = df.withColumn('ba', (split(col('value'), ',').getItem(5)))
df = df.withColumn('sa', (split(col('value'), ',').getItem(6)))
df = df.withColumn('hl', (split(col('value'), ',').getItem(7)))
df = df.withColumn('pcp', (split(col('value'), ',').getItem(8)))
df = df.withColumn('time', (split(col('value'), ',').getItem(9)))

def save_data(df, batch_id):
    # df.write.format('es')\
    #     .option("es.nodes", "elasticsearch")\
    #     .option("es.post", "9200")\
    #     .option("append")\
    #     .save("stocks/doc")
    # es.update(index="stocks", id=f"{df['symbol']}_{df['time']}",doc = df, doc_as_upsert=True)
    # es.index()
    doc_3 = {"city": "London", "country": "England"}
    # print(df)
    es.index(index="stocks", id=1, document=doc_3)
# df = df.select(from_json('value', schema).alias('json'))
# df.writeStream.foreachBatch(save_data).format('console').outputMode("append").start().awaitTermination()
df.writeStream.foreachBatch(save_data).start().awaitTermination()
