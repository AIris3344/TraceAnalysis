from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import matplotlib.pyplot as plt
# import seaborn as sb

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

df_machine_meta = spark.read.format("csv").option("header", "false").schema(schema_machine_meta).load(data_path + "machine_meta.csv")
print("machine_meta:")
df_machine_meta.show()



#
# cpu_dict = {}
#
# for row in df_machine_meta.select("machine_id", "time_stamp", "cpu_num").collect():
#     if cpu_dict.get(row.machine_id) == None:
#         cpu_dict[row.machine_id] = [[row.time_stamp], [row.cpu_num]]
#     else:
#         l1 = cpu_dict[row.machine_id][0]
#         l2 = cpu_dict[row.machine_id][1]
#         l1.append(row.time_stamp)
#         l2.append(row.cpu_num)
#         cpu_dict[row.machine_id] = [l1, l2]
#
# for key, value in cpu_dict.items():
#     plt.plot(value[0], value[1], label=key)
#
# plt.savefig("machine_meta.png")
# plt.show()
#
# print(cpu_dict)

# lists = df_machine_meta.select("machine_id").collect()
# print(lists)
# machine_ids = df_machine_meta.groupBy("machine_id").count().select("machine_id").orderBy("machine_id").cache()

# machine_ids.select("machine_id").foreach(lambda row: print(row))

# df_machine_meta.createOrReplaceTempView("machine_meta")
# sqlDF_machine_meta = spark.sql("SELECT * FROM machine_meta")
# sqlDF_machine_meta.show()

# sqlDF_machine_meta = spark.sql("SELECT machine_id FROM machine_meta group by machine_id order by machine_id")
# sqlDF_machine_meta.show()
# print(sqlDF_machine_meta.count())

#

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
spark.read.format("csv").option("header", "false").schema(schema_machine_usage).load(data_path + "machine_usage.csv").drop("mem_gps", "mkpi", "net_in", "net_out", "disk_io_percent").createOrReplaceTempView("machine_usage")
print("machine_usage:")
df_machine_usage = spark.sql("SELECT machine_id, time_stamp, cpu_util_percent, mem_util_percent, FLOOR(time_stamp / 86400) AS day, FLOOR(time_stamp / 900) AS minute FROM machine_usage")
df_machine_usage.show()
df_machine_usage.write.partitionBy('day', 'minute').parquet(data_path + "machine_usage.parquet")


# df_machine_usage = df_machine_usage.repartition(8, "day")
# df_machine_usage.show()
# df_machine_usage.foreachPartition

# cm_struct = StructType([StructField("machine_id", StringType(), True), StructField("cpu", DoubleType(), True), StructField("mem", DoubleType(), True), StructField("time_stamp", IntegerType(), True)])
# cm_frame = spark.createDataFrame(sc.emptyRDD(), cm_struct)

# spark.createDataFrame()
# cpu_dict = {}
# mem_dict = {}
# foreach_nums = range(1)
# for i in foreach_nums:
#     frame = spark.sql("SELECT machine_id, AVG(cpu_util_percent) AS cpu, AVG(mem_util_percent) AS mem, " + str(i) + " AS time_stamp FROM machine_usage WHERE time_stamp >= " + str(i * 15 * 60) + " AND time_stamp < " + str((i + 1) * 15 * 60) + " GROUP BY machine_id ORDER BY time_stamp")
#     frame.show()
#     cm_frame = cm_frame.unionAll(frame)
# cm_frame.show()



# df_cpu = df_machine_usage.select("machine_id", "cpu_util_percent").toPandas()


# cpu_dict = {}
#
# for row in df_machine_usage.select("machine_id", "time_stamp", "cpu_util_percent").collect():
#     if cpu_dict.get(row.machine_id) == None:
#         cpu_dict[row.machine_id] = [[row.time_stamp], [row.cpu_util_percent]]
#     else:
#         l1 = cpu_dict[row.machine_id][0]
#         l2 = cpu_dict[row.machine_id][1]
#         l1.append(row.time_stamp)
#         l2.append(row.cpu_util_percent)
#         cpu_dict[row.machine_id] = [l1, l2]
#
# for key, value in cpu_dict.items():
#     plt.plot(value[0], value[1], label=key)
#
# plt.savefig("machine_usage.png")
# plt.show()

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
df_container_meta = spark.read.format("csv").option("header", "false").schema(schema_container_meta).load(data_path + "container_meta.csv")
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
df_container_usage = spark.read.format("csv").option("header", "false").schema(schema_container_usage).load(data_path + "container_usage*.csv")
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
