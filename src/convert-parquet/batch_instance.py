from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

data_path = "hdfs://10.1.4.11:9000/user/hduser/"

spark = SparkSession.builder\
    .master("local[*]")\
    .appName("ConvertParquet")\
    .getOrCreate()

schema_batch_instance = StructType([ \
    StructField("instance_name", StringType(), True), \
    StructField("task_name", StringType(), True), \
    StructField("job_name", StringType(), True), \
    StructField("task_type", StringType(), True), \
    StructField("status", StringType(), True), \
    StructField("start_time", LongType(), True),\
    StructField("end_time", LongType(), True),\
    StructField("machine_id", StringType(), True), \
    StructField("seq_no", LongType(), True), \
    StructField("total_seq_no", LongType(), True), \
    StructField("cpu_avg", DoubleType(), True),\
    StructField("cpu_max", DoubleType(), True), \
    StructField("mem_avg", DoubleType(), True), \
    StructField("mem_max", DoubleType(), True)])

df_batch_instance = spark.read.format("csv").option("header", "false").schema(schema_batch_instance).load(data_path + "batch_instance*.csv")
print("batch_instance:")
df_batch_instance.show()

# Save as parquet file
df_batch_instance\
    .write\
    .parquet(data_path + "batch_instance_parquet")