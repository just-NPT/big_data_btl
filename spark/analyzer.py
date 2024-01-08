from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date
import pyspark.sql.functions as F
import sys
from datetime import datetime, timedelta

# replace host cassandra !!! 
spark = SparkSession.builder\
            .config("spark.app.name", "Analyzer")\
            .config("spark.master", "spark://spark-master:7077")\
            .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.3.0")\
            .config("spark.cassandra.connection.host", "cassandra")\
            .config("spark.cassandra.auth.username", "cassandra")\
            .config("spark.cassandra.auth.password", "cassandra")\
            .enableHiveSupport()\
            .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# read data from cassandra
result_df = spark.read.format('org.apache.spark.sql.cassandra') \
    .options(table='review_data', keyspace='review') \
    .load()

print(result_df.schema)

ten_days_ago = (datetime.today() - timedelta(days=10)).replace(hour=0, minute=0, second=0)
agg_last10_df = result_df.filter(F.col("datetime_occured") > ten_days_ago).\
                    groupBy("product_id").agg(F.avg("scores").alias("avg_scores"),
                                                 F.avg("star_rating").alias("avg_rating"),
                                                 )
agg_df = result_df.\
                    groupBy("product_id").agg(F.avg("scores").alias("avg_scores"),
                                                 F.avg("star_rating").alias("avg_rating"),
                                                 )


def statistic ():
    #Thong ke
    print("Top 10 san pham co danh gia cao nhat la")
    agg_df.orderBy(F.desc("avg_rating")).show(10)

    print("Top 10 san pham co diem sentiment cao nhat la ")
    agg_df.orderBy(F.desc("avg_scores")).show(10)

    print("Top 10 san pham co danh gia cao nhat trong 10 ngay gan nhat la")
    agg_last10_df.orderBy(F.desc("avg_rating")).show(10)

    print("Top 10 san pham co diem sentiment cao nhat la ")
    agg_last10_df.orderBy(F.desc("avg_scores")).show(10)

def history (product_id):
    #Lich su
    #Xem lich su 1 ma
    print(f"Lich su review cua ma san pham {product_id} gan nhat")
    result_df.filter(result_df.product_id == product_id).show(10)

if (sys.argv[1] == "statistic"):
    statistic()
elif (sys.argv[1] == "history"):
    history(sys.argv[2])







