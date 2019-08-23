from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

data_path = "hdfs://10.1.4.11:9000/user/hduser/"

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("ConvertParquet") \
    .getOrCreate()

schema_batch_task = StructType([\
    StructField("task_name", StringType(), True),\
    StructField("instance_num", LongType(), True),\
    StructField("job_name", StringType(), True), \
    StructField("task_type", StringType(), True), \
    StructField("status", StringType(), True), \
    StructField("start_time", LongType(), True),\
    StructField("end_time", LongType(), True),\
    StructField("plan_cpu", DoubleType(), True), \
    StructField("plan_mem", DoubleType(), True)])

df_batch_task = spark \
    .read \
    .csv(data_path + "batch_task.csv", schema=schema_batch_task, header=False) \

print("batch_task:")
df_batch_task.show()

# Save as parquet file
df_batch_task \
    .write \
    .parquet(data_path + "batch_task_parquet")
