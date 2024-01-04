from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    date_trunc, col, split, decode, from_json, to_json
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

schema = StructType([
    StructField("review_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("product_id", StringType(), True)
])

spark.sparkContext.setLogLevel("WARN")
es = Elasticsearch(hosts='http://elasticsearch:9200')
# if not(es.indices.exists(index="realtime")):
# es.indices.create(index="checkout")

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "checkout") \
    .option("startingOffsets", "latest") \
    .load()

schema = StructType([
    StructField("checkout_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("price", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("num_product", StringType(), True),
    StructField("datetime_occured", StringType(), True)
])

# df.selectExpr("CAST(value AS STRING)").writeStream \
#     .format("console") \
#     .outputMode("append") \
#     .start() \
#     .awaitTermination()

df = df.selectExpr("CAST(value AS STRING)")
df = df.select(from_json('value', schema).alias('value'))

# query = df.select(
#     col("json.checkout_id"),
#     col("json.user_id"),
#     col("json.product_id"),
#     col("json.price"),
#     col("json.payment_method"),
#     col("json.num_product"),
#     col("json.datetime_occured")
# ).writeStream \
#     .format("console") \
#     .outputMode("append") \
#     .start()

# query.awaitTermination()

print(df.schema)

def save_data(df, batch_id):
    data = df.collect()
    index = 0
    for row in data:
        index = index + 1 
        doc_send = {
            "checkout_id": row['value']['checkout_id'],
            "user_id": row['value']['user_id'],
            "product_id": row['value']['product_id'],
            "price": row['value']['price'],
            "payment_method": row['value']['payment_method'],
            "num_product": row['value']['num_product'],
            "datetime_occured": row['value']['datetime_occured']
        }
        
        es.index(index="checkout_test", id=f"{batch_id}_{index}", document=doc_send)


df.writeStream.foreachBatch(save_data).start().awaitTermination()

