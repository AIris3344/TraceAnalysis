import matplotlib.pyplot as plt
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from matplotlib.ticker import FormatStrFormatter

data_path = "hdfs://10.1.4.11:9000/user/hduser/"

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("TraceAnalysis") \
    .config("spark.driver.memory", "8g") \
    .getOrCreate()

# Save data for every day
for i in range(8):
    spark.read.parquet(data_path + "machine_usage.parquet/day=" + str(i)).createOrReplaceTempView("machine_usage")
    df = spark.sql("SELECT 'day-" + str(i) + "' AS day, "
                                        "((minute * 15 - " + str(i * 24 * 60) + ") / 60) AS time_h, "
                                        "AVG(cpu_util_percent) AS cpu_avg, "
                                        "AVG(mem_util_percent) AS mem_avg "
                                        "FROM machine_usage "
                                        "GROUP BY minute ")
    df.write.csv(data_path + "machine_usage_24hour/day-" + str(i) + ".csv")

schema = StructType([\
    StructField("day", StringType(), True),\
    StructField("time_h", DoubleType(), True),\
    StructField("cpu_util_percent", DoubleType(), True),\
    StructField("mem_util_percent", DoubleType(), True)])

# Plot CPU average usage
plt.figure(figsize=[12, 6])
plt.xlabel('Time(hours)')
plt.ylabel('CPU usage')
for i in range(8):
    df = spark.read.csv(data_path + "machine_usage_24hour/day-" + str(i) + ".csv", schema=schema).orderBy("day", "time_h")
    rows = df.count()
    plt.plot(
        df.select("time_h").toPandas().values.reshape(rows),
        df.select("cpu_util_percent").toPandas().values.reshape(rows),
        label="day-" + str(i)
    )
plt.xlim(24)
plt.xticks(np.arange(0, 25, 2))
plt.gca().yaxis.set_major_formatter(FormatStrFormatter("%.0f%%"))
plt.legend(loc="upper right")
plt.grid()
plt.show()

# Plot Memory average usage
plt.figure(figsize=[12, 8])
plt.xlabel('Time(hours)')
plt.ylabel('Memory usage')
for i in range(8):
    df = spark.read.csv("day-" + str(i) + ".csv", schema=schema).orderBy("day", "time_h")
    rows = df.count()
    plt.plot(
        df.select("time_h").toPandas().values.reshape(rows),
        df.select("mem_util_percent").toPandas().values.reshape(rows),
        label="day-" + str(i)
    )
plt.xlim(24)
plt.xticks(np.arange(0, 25, 2))
plt.gca().yaxis.set_major_formatter(FormatStrFormatter("%.0f%%"))
plt.legend(loc='upper right')
plt.grid()
plt.show()
