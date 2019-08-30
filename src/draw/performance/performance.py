#%%
import os
import numpy as np
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession

data_path = "hdfs://10.1.4.11:9000/user/hduser/"
write_path = os.environ['HOME'] + "/data/"

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Performance") \
    .getOrCreate()

container_usage = spark.read.parquet(data_path + "container_usage_parquet")
container_usage.show()
container_usage.createOrReplaceTempView("container_usage")

container_usage = spark.sql("SELECT machine_id, container_id, FLOOR(time_stamp / (60 * 60)) AS t_hour, FLOOR(time_stamp / (60 * 60 * 24)) AS t_day, mem_util_percent as mem, cpu_util_percent as cpu, mpki, cpi FROM container_usage")
container_usage.write.parquet(write_path + "container_usage/staging")

#%%
# Write colocated data

container_usage = spark.read.parquet(write_path + "container_usage/staging")
container_usage.show()
container_usage = container_usage.groupBy("container_id", "t_hour").agg({"mpki": "avg", "cpi": "avg", "mem": "avg", "cpu": "avg"})
container_usage = container_usage.select("container_id", "t_hour", container_usage["avg(mpki)"].alias("mpki"), container_usage["avg(cpi)"].alias("cpi"), container_usage["avg(mem)"].alias("mem"), container_usage["avg(cpu)"].alias("cpu")).dropna()
container_usage.write.parquet(write_path + "container_usage/performance_day")

#%%
container = spark.read.parquet(write_path + "container_usage/performance_day").orderBy("t_hour")
container = container.groupBy("t_hour") \
    .agg({"mem": "avg", "cpu": "avg", "mpki": "avg", "cpi": "avg"}) \
    .orderBy("t_hour")
container = container.select((container["t_hour"] / 24).alias("t_hour"), \
        container["avg(mem)"].alias("mem"), \
        container["avg(cpu)"].alias("cpu"), \
        container["avg(mpki)"].alias("mpki"), \
        container["avg(cpi)"].alias("cpi"), \
    )
container.show()

#%%
## Plot CPU and Memory 

fig, ax = plt.subplots()

x = np.reshape(container.select("t_hour").collect(), -1) - 1
y = np.reshape(container.select("cpu").collect(), -1)

ax.plot(x, y, label="CPU")

x = np.reshape(container.select("t_hour").collect(), -1) - 1
y = np.reshape(container.select("mem").collect(), -1)
ax.plot(x, y, label="Memory")

ax.set_ylabel("Utilization")
ax.set_xlabel("Time(days)")
ax.set_xlim(0, 8)
ax.set_ylim(0, 100)
ax.set_xticks(np.arange(0, 9, 1))
ax.set_yticks(np.arange(0, 110, 10))
ax.set_yticklabels(["{:.1f}%".format(y) for y in ax.get_yticks()])
ax.legend(ncol=2, bbox_to_anchor=(0.3, 1), loc='lower left', fontsize='small')

plt.grid(linestyle='dotted')
plt.show()

#%%
## Plot CPI
fig, ax = plt.subplots()
x = np.reshape(container.select("t_hour").collect(), -1) - 1
y = np.reshape(container.select("cpi").collect(), -1)

ax.plot(x, y, label="CPI")

ax.set_xlabel("Time(days)")
ax.set_ylabel("Cycles / Instruction")

plt.grid(linestyle='dotted')
plt.show()

#%%
## Plot MPKI
fig, ax = plt.subplots()
x = np.reshape(container.select("t_hour").collect(), -1) - 1
y = np.reshape(container.select("mpki").collect(), -1) / 1000

ax.plot(x, y, label="MPKI")

ax.set_xlabel("Time(days)")
ax.set_ylabel("Cache miss / Instruction")

plt.grid(linestyle='dotted')
plt.show()
