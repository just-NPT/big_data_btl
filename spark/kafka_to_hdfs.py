import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    date_trunc, col, split, decode, from_json, to_json
)
from pyspark.sql.types import *

spark = SparkSession.builder\
    .config("spak.app.name", "StreamStock")\
    .config("spark.master", "local[*]")\
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.2,org.elasticsearch:elasticsearch-spark-30_2.12:7.15.1")\
    .enableHiveSupport()\
    .getOrCreate()

df = spark.readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "kafka:29092")\
    .option("subscribe", "review")\
    .option("startingOffsets", "latest")\
    .load()

query = df.writeStream\
            .outputMode("append")\
            .trigger(processingTime='60 seconds')\
            .option("checkpointLocation", "hdfs://namenode:9000/checkpoints")\
            .option("path", "hdfs://namenode:9000/test")\
            .format("parquet")\
            .start()

query.awaitTermination()

# try:
#     while True:
#         msg = consumer.poll(10.0)
#         if msg is None:
#             continue
#         if msg.error():
#             if msg.error().code() == KafkaError._PARTITION_EOF:
#                 continue
#             else:
#                 print(msg.error())
#                 break

#         json_data = json.loads(msg.value().decode('utf-8'))
#         df = spark.createDataFrame(json_data)

#         df.write.mode('append').parquet('hdfs://namenode:9000/test/review.parquet')
# except:
#     print("LOL")
