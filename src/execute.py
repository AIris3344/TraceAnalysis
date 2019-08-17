from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import matplotlib.pyplot as plt

data_path = "hdfs://10.1.4.11:9000/user/hduser/"

spark = SparkSession.builder\
    .master("local[*]")\
    .appName("TraceAnalysis")\
    .getOrCreate()


machine_usage = spark.read.parquet(data_path + "machine_usage.parquet")
print("machine_usage:")
machine_usage.show()

cm_struct = StructType([StructField("machine_id", StringType(), True), StructField("cpu", DoubleType(), True), StructField("mem", DoubleType(), True), StructField("time_stamp", IntegerType(), True)])
cm_frame = spark.createDataFrame(sc.emptyRDD(), cm_struct)



# df_machine_usage = spark.sql("SELECT machine_id, time_stamp, cpu_util_percent, mem_util_percent, FLOOR(time_stamp / 86400) AS day, FLOOR(time_stamp / 900) AS minute FROM machine_usage")
# df_machine_usage.show()
# df_machine_usage.write.partitionBy('day', 'minute').parquet(data_path + "machine_usage.parquet")
result_dict = {}
for day in range(8):
    for minute in range(96):
        spark.read.parquet(data_path + "machine_usage.parquet/day=" + str(day) + "/minute=" + str(minute * 15) + "/*").groupBy("machine_id").agg({"cpu_util_percent": "avg"}).show().
