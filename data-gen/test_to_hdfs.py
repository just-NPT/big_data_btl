import datetime
import json
from logging import Logger
import uuid
from confluent_kafka import Consumer, KafkaError
from hdfs import InsecureClient
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import fs
import os

# Kafka configuration
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
hdfs_destination_path = '/test/'

# Create Kafka consumer
consumer = Consumer(kafka_config)
consumer.subscribe([kafka_topic])

# os.environ['ARROW_LIBHDFS_DIR'] = 'D:\Anaconda3-Windows\Lib\site-packages'

# Create HDFS client
hdfs_client = InsecureClient('http://localhost:9870', user='root')
hdfs_filesystem = fs.HadoopFileSystem(host = 'localhost',port = 9870)
try:
    while True:
        msg = consumer.poll(10.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break

        # Process the Kafka message
        json_data = json.loads(msg.value().decode('utf-8'))
        df = pd.DataFrame.from_dict([json_data])
        print(json_data)
        # table = pa.Table.from_pandas(df)
        # parquet_file = 'temp.parquet'
        # pq.write_table(table, parquet_file)

        current_time = datetime.datetime.now()
        hdfs_filename = "/clicks/" +\
            str(current_time.year) + "/" +\
            str(current_time.month) + "/" +\
            str(current_time.day) + "/"\
            f"reviews.parquet"
        try:
            table = pq.read_table(hdfs_filename, filesystem=hdfs_filesystem)
            new_data_table = pa.Table.from_pandas(df, schema=table.schema)

            merged_table = pa.concat_tables([table, new_data_table])
        # flush_status = hdfs_client.upload(hdfs_filename, parquet_file)
            with pq.ParquetWriter(hdfs_filename, merged_table.schema, filesystem=hdfs_client) as writer:
                writer.write_table(merged_table)
        except:
            table = pa.Table.from_pandas(df)
            with pq.ParquetWriter(hdfs_filename, table.schema, filesystem=hdfs_client) as writer:
                writer.write_table(table)

            print("LOL")
        # if flush_status:
        #     Logger.info(
        #         f"Flush file {parquet_file} to hdfs as {hdfs_filename} successfully")
        # else:
        #     raise RuntimeError(f"Failed to flush file {parquet_file} to hdfs")

except KeyboardInterrupt:
    pass
finally:
    consumer.close()
