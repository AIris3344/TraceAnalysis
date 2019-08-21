from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

data_path = "hdfs://10.1.4.11:9000/user/hduser/"

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("ConvertParquet") \
    .getOrCreate()

schema_machine_usage = StructType([ \
    StructField("machine_id", StringType(), True), \
    StructField("time_stamp", DoubleType(), True), \
    StructField("cpu_util_percent", IntegerType(), True), \
    StructField("mem_util_percent", IntegerType(), True), \
    StructField("mem_gps", DoubleType(), True), \
    StructField("mkpi", LongType(), True), \
    StructField("net_in", DoubleType(), True), \
    StructField("net_out", DoubleType(), True), \
    StructField("disk_io_percent", DoubleType(), True)])

df_machine_usage = spark \
    .read \
    .csv(data_path + "machine_usage.csv", schema=schema_machine_usage, header=False) \
    .drop("mem_gps", "net_in", "net_out", "disk_io_percent")

print("machine_usage:")
df_machine_usage.show()

# Save as parquet file
df_machine_usage \
    .write \
    .parquet(data_path + "machine_usage_parquet")
