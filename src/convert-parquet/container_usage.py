from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

data_path = "hdfs://10.1.4.11:9000/user/hduser/"

spark = SparkSession.builder\
    .master("local[*]")\
    .appName("ConvertParquet")\
    .getOrCreate()

schema_container_usage = StructType([\
    StructField("container_id", StringType(), True),\
    StructField("machine_id", StringType(), True),\
    StructField("time_stamp", DoubleType(), True), \
    StructField("cpu_util_percent", LongType(), True), \
    StructField("mem_util_percent", LongType(), True), \
    StructField("cpi", DoubleType(), True),\
    StructField("mem_gps", DoubleType(), True),\
    StructField("mpki", LongType(), True), \
    StructField("net_in", DoubleType(), True), \
    StructField("net_out", DoubleType(), True),\
    StructField("disk_io_percent", DoubleType(), True)])

df_container_usage = spark.read\
    .format("csv")\
    .option("header", "false")\
    .schema(schema_container_usage)\
    .load(data_path + "container_usage*.csv")\
    .drop("mem_gps", "net_in", "net_out", "disk_io_percent")

df_container_usage.show()

# Save as parquet file
df_container_usage\
    .write\
    .parquet(data_path + "container_usage_parquet")