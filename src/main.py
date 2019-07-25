from pyspark.sql import SparkSession
from pyspark.sql.types import *
from functools import partial

def split(row, dataFrame, prefix):
    dataFrame.filter("machine_id=" + row.machine_id)\
        .write\
        .csv("/mnt/1AF492ABF49288A1/MScProject/split/" + prefix + "_" + row.machine_id, mode="overwrite", header="true")


spark = SparkSession.builder\
    .master("local")\
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

df_machine_meta = spark.read.format("csv").option("header", "false").schema(schema_machine_meta).load("/mnt/1AF492ABF49288A1/MScProject/machine_meta.csv")
machine_ids = df_machine_meta.groupBy("machine_id").count().select("machine_id").orderBy("machine_id").cache()

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
    StructField("cpu_util_percent", LongType(), True),\
    StructField("mem_util_percent", LongType(), True),\
    StructField("mem_gps", DoubleType(), True),\
    StructField("mkpi", LongType(), True),\
    StructField("net_in", DoubleType(), True),\
    StructField("net_out", DoubleType(), True),\
    StructField("disk_io_percent", DoubleType(), True)])

df_machine_usage = spark.read.format("csv").option("header", "false").schema(schema_machine_usage).load("/mnt/1AF492ABF49288A1/MScProject/machine_usage.csv")

for i in machine_ids.collect():
    df_machine_usage.orderBy("machine_id").filter(df_machine_usage["machine_id"] == i.machine_id).show()
# df_machine_usage.filter(df_machine_usage['machine_id'] == "m_967").show()

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