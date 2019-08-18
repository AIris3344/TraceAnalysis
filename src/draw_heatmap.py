import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import seaborn as sns
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.types import *

data_path = "hdfs://10.1.4.11:9000/user/hduser/"

spark = SparkSession.builder\
    .master("local[*]")\
    .appName("TraceAnalysis")\
    .getOrCreate()

schema = StructType([\
    StructField("machine_id", IntegerType(), True),\
    StructField("minute", IntegerType(), True),\
    StructField("mem_util_percent", DoubleType(), True),\
    StructField("cpu_util_percent", DoubleType(), True)])

# .load(data_path + "machine_usage_output.csv/part-00000-25f3311a-92ba-455d-9c43-5edfb1907f64-c000.csv")
machine_usage = spark.read\
    .format("csv")\
    .option("header", "false")\
    .schema(schema)\
    .load(data_path + "machine_usage_output.csv")

df = machine_usage\
    .select("machine_id", (machine_usage.minute / 96).alias("day"), "cpu_util_percent", "mem_util_percent")\
    .toPandas()

# print(df.tail())

cpu = df[['machine_id', "day", 'cpu_util_percent']].pivot(index="machine_id", columns="day", values="cpu_util_percent")
mem = df[['machine_id', "day", 'mem_util_percent']].pivot(index="machine_id", columns="day", values="mem_util_percent")

ax = sns.heatmap(cpu, vmin=0, cmap="gist_rainbow_r")
ax.invert_yaxis()
ax.set_xlabel("Time(days) - CPU")
ax.set_ylabel("Machine ID")
ax.set_xlim(0, 8)
ax.set_ylim(0, 4500)
ax.set_xticks(np.arange(0, 8, 1))
ax.set_yticks(np.arange(0, 4500, 500))
ax.set_xticklabels(np.arange(0, 8, 1))
ax.set_yticklabels(np.arange(0, 4500, 500))
plt.show()

ax = sns.heatmap(mem, vmin=0, cmap="gist_rainbow_r")
ax.invert_yaxis()
ax.set_xlabel("Time(days) - Memory")
ax.set_ylabel("Machine ID")
ax.set_xlim(0, 8)
ax.set_ylim(0, 4500)
ax.set_xticks(np.arange(0, 8, 1))
ax.set_yticks(np.arange(0, 4500, 500))
ax.set_xticklabels(np.arange(0, 8, 1))
ax.set_yticklabels(np.arange(0, 4500, 500))
plt.show()
