from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

data_path = "hdfs://10.1.4.11:9000/user/hduser/"

spark = SparkSession.builder\
    .master("local[*]")\
    .appName("ConvertParquet")\
    .getOrCreate()

schema_container_meta = StructType([\
    StructField("container_id", StringType(), True),\
    StructField("machine_id", StringType(), True),\
    StructField("time_stamp", IntegerType(), True),\
    StructField("app_du", StringType(), True),\
    StructField("status", StringType(), True),\
    StructField("cpu_request", IntegerType(), True),\
    StructField("cpu_limit", IntegerType(), True),\
    StructField("mem_size", DoubleType(), True)])
df_container_meta = spark.read.csv(data_path + "container_meta.csv", header=False, schema=schema_container_meta)
print("container_meta:")
df_container_meta.show()

# Save as parquet file
df_container_meta\
    .write\
    .parquet(data_path + "container_meta_parquet")