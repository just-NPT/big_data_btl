import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    date_trunc, col, split, decode, from_json, to_json
)
# from confluent_kafka import Consumer, KafkaError
# import pandas as pd
from pyspark.sql.types import *
# fix version 2.12:3.3.1  (3.3.1 is version spark)

kafka_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'consumer_group_test',
    'auto.offset.reset': 'earliest'
}

# HDFS configuration
hdfs_config = {
    'url': 'http://localhost:9870',
    'user': 'root'
}

# Kafka topic to consume from
kafka_topic = 'review'

# HDFS destination path
# hdfs_destination_path = '/test/'

# # Create Kafka consumer
# consumer = Consumer(kafka_config)
# consumer.subscribe([kafka_topic])

spark = SparkSession.builder\
    .config("spak.app.name", "StreamStock")\
    .config("spark.master", "local[*]")\
    .config("spark.sql.session.timeZone", "Asia/Hanoi") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.2,org.elasticsearch:elasticsearch-spark-30_2.12:7.15.1")\
    .enableHiveSupport()\
    .getOrCreate()

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "review") \
    .option("startingOffsets", "latest") \
    .load()

query = df.writeStream\
            .outputMode("append")\
            .trigger(processingTime='60 seconds')\
            .option("checkpointLocation", "hdfs://namenode:9000/checkpoints")\
            .option("path", "hdfs://namenode:9000/test/test.parquet")\
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
