from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

data_path = "hdfs://10.1.4.11:9000/user/hduser/"

spark = SparkSession.builder\
    .master("local[*]")\
    .appName("TraceAnalysis")\
    .getOrCreate()

sc = spark.sparkContext
#
schema_machine_meta = StructType([\
    StructField("machine_id", StringType(), True),\
    StructField("time_stamp", LongType(), True),\
    StructField("failure_domain_1", LongType(), True),\
    StructField("failure_domain_2", StringType(), True),\
    StructField("cpu_num", LongType(), True),\
    StructField("mem_size", LongType(), True),\
    StructField("status", StringType(), True),])

df_machine_meta = spark.read.csv(data_path + "machine_meta.csv", header=False, schema=schema_machine_meta)
print("machine_meta:")
df_machine_meta.show()


schema_machine_usage = StructType([\
    StructField("machine_id", StringType(), True),\
    StructField("time_stamp", DoubleType(), True),\
    StructField("cpu_util_percent", IntegerType(), True),\
    StructField("mem_util_percent", IntegerType(), True),\
    StructField("mem_gps", DoubleType(), True),\
    StructField("mkpi", LongType(), True),\
    StructField("net_in", DoubleType(), True),\
    StructField("net_out", DoubleType(), True),\
    StructField("disk_io_percent", DoubleType(), True)])
spark.read.format("csv").option("header", "false").schema(schema_machine_usage).load(data_path + "machine_usage.csv").createOrReplaceTempView("machine_usage")
print("machine_usage:")
spark.sql("select * from machine_usage").show()
df_machine_usage = spark.sql("SELECT CAST(SUBSTRING(machine_id, 3, 4) AS int) AS machine_id, time_stamp, cpu_util_percent, mem_util_percent, FLOOR(time_stamp / 900) AS minute FROM machine_usage")
df_machine_usage.show()

"""
df_machine_usage.write.partitionBy('day', 'minute').parquet(data_path + "machine_usage.parquet")

df_machine_usage.select("machine_id", "minute", "cpu_util_percent", "mem_util_percent")\
    .groupBy("machine_id", "minute")\
    .agg({"cpu_util_percent": "avg", "mem_util_percent": "avg"})\
    .orderBy("machine_id", "minute")\
    .write.csv(data_path + "machine_usage_output.csv")
    
"""

#
schema_container_meta = StructType([\
    StructField("container_id", StringType(), True),\
    StructField("machine_id", StringType(), True),\
    StructField("time_stamp", LongType(), True),\
    StructField("app_du", StringType(), True),\
    StructField("status", StringType(), True),\
    StructField("cpu_request", LongType(), True),\
    StructField("cpu_limit", LongType(), True),\
    StructField("mem_size", DoubleType(), True)])
df_container_meta = spark.read.csv(data_path + "container_meta.csv", header=False, schema=schema_container_meta)
print("container_mata:")
df_container_meta.show()


#
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
df_container_usage = spark.read.csv(data_path + "container_usage*.csv", header=False, schema=schema_container_usage)
print("container_usage:")
df_container_usage.show()

#
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

df_batch_task = spark.read.format("csv").option("header", "false").schema(schema_batch_task).load(data_path + "batch_task.csv")
print("batch_task:")
df_batch_task.show()

#
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


# for i in machine_ids.collect():
#     df_machine_usage.orderBy("machine_id").filter(df_machine_usage["machine_id"] == i.machine_id).show()
#


# df_machine_usage.filter((df_machine_usage['machine_id'] == "m_1") & (df_machine_usage['time_stamp'] == '683653')).show()

# machine_ids.select("machine_id").foreach(lambda row: print(row))

# df_machine_usage.createOrReplaceTempView("machine_usage")
# sqlDF_machine_usage = spark.sql("SELECT * FROM machine_usage group by machine_id")

# df_machine_usage.filter(df_machine_usage['machine_id'] == 'm_967').show()



#
# machine_list = sqlDF_machine_meta.collect()
# print(machine_list)

# sqlDF_machine_meta.foreach(lambda row: sqlDF_machine_usage.filter("machine_id=" + row.machine_id).write.csv("/mnt/1AF492ABF49288A1/MScProject/split/machine_usage_" + row.machine_id, mode="overwrite", header="true"))
# machine_ids.foreach(lambda row: print(row.machine_id))


spark.stop()
