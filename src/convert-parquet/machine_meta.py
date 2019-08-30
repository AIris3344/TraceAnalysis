from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

data_path = "hdfs://10.1.4.11:9000/user/hduser/"

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("ConvertParquet") \
    .getOrCreate()

schema_machine_meta = StructType([\
    StructField("machine_id", StringType(), True),\
    StructField("time_stamp", IntegerType(), True),\
    StructField("failure_domain_1", IntegerType(), True),\
    StructField("failure_domain_2", StringType(), True),\
    StructField("cpu_num", IntegerType(), True),\
    StructField("mem_size", IntegerType(), True),\
    StructField("status", StringType(), True),])

df_machine_meta = spark \
    .read \
    .csv(data_path + "machine_meta.csv", header=False, schema=schema_machine_meta) \
    .drop("failure_domain_1", "failure_domain_2")
print("machine_meta:")
df_machine_meta.show()

# Save as parquet file
df_machine_meta \
    .write \
    .parquet(data_path + "machine_meta_parquet")
