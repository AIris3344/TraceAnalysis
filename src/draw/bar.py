import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import SparkSession
from pyspark.sql.types import *
sns.set()

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
    .load(data_path + "machine_usage_output.csv/part-00000-25f3311a-92ba-455d-9c43-5edfb1907f64-c000.csv")

machine_usage = machine_usage\
    .groupBy("minute")\
    .agg({"mem_util_percent": "avg", "cpu_util_percent": "avg"})

machine_usage = machine_usage\
    .select((machine_usage["avg(mem_util_percent)"]).alias("mem_util_percent"), (machine_usage["avg(cpu_util_percent)"]).alias("cpu_util_percent"))

machine_usage.show()

# Draw memory average usage
ax = sns.distplot(machine_usage.select("mem_util_percent").collect(), bins=5)
ax.set_xlabel("Memmory usage")
ax.set_ylabel("Fraction time")
x_vals = ax.get_xticks()
y_vals = ax.get_yticks()
ax.set_xticklabels(["{:.0f}%".format(x) for x in x_vals])
ax.set_yticklabels(["{:.0f}%".format(y * 100) for y in y_vals])
plt.show()

# Draw CPU average usage
ax = sns.distplot(machine_usage.select("cpu_util_percent").collect(), bins=5)
ax.set_xlabel("CPU usage")
ax.set_ylabel("Fraction time")
x_vals = ax.get_xticks()
y_vals = ax.get_yticks()
ax.set_xticklabels(["{:.0f}%".format(x) for x in x_vals])
ax.set_yticklabels(["{:.1f}%".format(y * 100) for y in y_vals])
plt.show()

"""
machine_usage = spark.sql("SELECT (FLOOR(mem_util_percent / 10) * 10) AS mem_group, (FLOOR(cpu_util_percent / 10) * 10) AS cpu_group FROM machine_usage")
# machine_usage.cache()

mem_average = machine_usage.select("mem_group").groupBy("mem_group").agg({"mem_group": "count"})
mem_average.show()
mem_average = mem_average.select("mem_group", (mem_average["count(mem_group)"] / 768 * 100).alias("fraction"))
mem_average.show()
"""
"""
ax = sns.distplot(np.reshape(machine_usage.select("mem_util_percent").collect(), total), bins=10, kde=True)
total = 1
ax.set_xlabel("Memmory usage")
ax.set_ylabel("Fraction time")
ax.set_xlim(0, 1)
ax.set_ylim(0, total)
ax.set_xticks(np.arange(0, 1, 0.1))
ax.set_yticks(np.arange(0, total, total * 0.05))
ax.set_xticklabels(["{:.0f}%".format(x * 100) for x in np.arange(0, 1, 0.1)])
# ax.set_yticklabels(["{:.0f}%".format(y / total * 100) for y in np.arange(0, total, total * 0.05)])
plt.show()

ax = sns.distplot(machine_usage.select("cpu_util_percent").collect(), bins=10, kde=True)
ax.set_xlabel("CPU usage")
ax.set_ylabel("Fraction time")
ax.set_xlim(0, 1)
ax.set_ylim(0, total)
ax.set_xticks(np.arange(0, 1, 0.1))
ax.set_yticks(np.arange(0, total, total * 0.05))
ax.set_xticklabels(["{:.0f}%".format(x * 100) for x in np.arange(0, 1, 0.1)])
ax.set_yticklabels(["{:.0f}%".format(y / total * 100) for y in np.arange(0, total, total * 0.05)])
plt.show()
"""
"""
df = machine_usage\
    .toPandas()

# print(df.tail())

average_cpu = df[["minute", 'cpu_util_percent']].pivot(index="machine_id", columns="day", values="cpu_util_percent")
average_mem = df[["minute", 'mem_util_percent']].pivot(index="machine_id", columns="day", values="mem_util_percent")

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
"""