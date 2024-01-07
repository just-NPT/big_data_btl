import datetime
import json
from logging import Logger
import logging
import uuid
from confluent_kafka import Consumer, KafkaError
from hdfs import InsecureClient
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import fs
import os
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
nltk.download('vader_lexicon')

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

logger = logging.getLogger('consumer')

# os.environ['ARROW_LIBHDFS_DIR'] = 'D:\Anaconda3-Windows\Lib\site-packages'

# Create HDFS client
hdfs_client = InsecureClient('http://localhost:9870', user='root')
# hdfs_filesystem = fs.HadoopFileSystem(host = 'localhost',port = 9870)
msg_count = 0
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

        sia = SentimentIntensityAnalyzer()
        # Create column with header and body text
        def headline(row):
            header = row['review_headline']
            body = row['review_body']
        
            return f'{header}: {body}'

        df['review'] = df.apply(headline, axis=1)
        df['scores'] = df['review'].apply(lambda x: sia.polarity_scores(x)['compound'])

        print(df)
        # table = pa.Table.from_pandas(df)
        # parquet_file = 'temp.parquet'
        # pq.write_table(table, parquet_file)

        current_time = datetime.datetime.now()
        hdfs_filename = "/reviews/" +\
            str(current_time.year) + "/" +\
            str(current_time.month) + "/" +\
            str(current_time.day) + "/"\
            f"reviews.{uuid.uuid4()}"
        
        msg_count += 1
        print(f"flushing message {msg_count} to hdfs...")
        table = pa.Table.from_pandas(df)
        parquet_file = 'temp.parquet'
        pq.write_table(table, parquet_file)
        
        logger.info(
            f"Starting flush file {parquet_file} to hdfs")
        flush_status = hdfs_client.upload(hdfs_filename, parquet_file)
        os.remove(parquet_file)

        # if flush_status:
        #     Logger.info(
        #         f"Flush file {parquet_file} to hdfs as {hdfs_filename} successfully")
        # else:
        #     raise RuntimeError(f"Failed to flush file {parquet_file} to hdfs")

except KeyboardInterrupt:
    pass
finally:
    consumer.close()
