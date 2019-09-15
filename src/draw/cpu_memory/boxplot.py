#%%
import os
import numpy as np
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession

data_path = "hdfs://10.1.4.11:9000/user/hduser/"
write_path = os.environ['HOME'] + "/data/"

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("BoxPlot") \
    .getOrCreate()

machine_usage = spark.read.parquet(data_path + "machine_usage_parquet")
machine_usage.show()
machine_usage.createOrReplaceTempView("machine_usage")

machine_usage = spark.sql("SELECT machine_id, FLOOR(time_stamp / (60 * 60)) AS t_hour, mem_util_percent as mem, cpu_util_percent as cpu, mkpi as mpki FROM machine_usage")
machine_usage.write.parquet(write_path + "machine_usage/staging")

#%%
# Write colocated data
machine_usage = spark.read.parquet(write_path + "machine_usage/staging")

colocated = np.genfromtxt(write_path + "colocated.csv", dtype='str')
machine_usage = machine_usage.filter(machine_usage["machine_id"].isin(colocated.tolist()))
basic = machine_usage.groupBy("t_hour", "machine_id").agg({"cpu": "avg", "mem": "avg", "mpki": "avg"})
basic = basic.select("machine_id", "t_hour", basic["avg(cpu)"].alias("cpu"), basic["avg(mem)"].alias("mem"), basic["avg(mpki)"].alias("mpki"))

basic.write.parquet(write_path + "machine_usage/staging_colocated")

#%%
machine_usage = spark.read.parquet(write_path + "machine_usage/staging")
online = np.genfromtxt(write_path + "online_only.csv", dtype='str')
online = machine_usage.filter(machine_usage["machine_id"].isin(online.tolist()))

machine_usage = spark.read.parquet(write_path + "machine_usage/staging")
batch = np.genfromtxt(write_path + "batch_only.csv", dtype='str')
batch = machine_usage.filter(machine_usage["machine_id"].isin(batch.tolist()))

colocated = spark.read.parquet(write_path + "machine_usage/staging_colocated")
#%%
###
# Plot cpu
colocted_cpu = np.reshape(colocated.select("cpu").dropna().collect(), -1)
batch_cpu = np.reshape(batch.select("cpu").dropna().collect(), -1)
online_cpu = np.reshape(online.select("cpu").dropna().collect(), -1)

plt.ylim((0,100))
plt.yticks(np.arange(0, 110, 10))

ax = plt.gca()
ax.set_yticklabels(["{:.0f}%".format(y) for y in ax.get_yticks()])

data = [batch_cpu, colocted_cpu, online_cpu]

plt.boxplot(data, flierprops={"marker" :'_'}, whis=[5, 95], labels=["Batch Only", "Co-located", "Online Only"])
plt.grid(linestyle='dotted')
plt.show()

###
# Plot mem
colocted_mem = np.reshape(colocated.select("mem").dropna().collect(), -1)
batch_mem = np.reshape(batch.select("mem").dropna().collect(), -1)
online_mem = np.reshape(online.select("mem").dropna().collect(), -1)

plt.ylim((0,100))
plt.yticks(np.arange(0, 110, 10))

ax = plt.gca()
ax.set_yticklabels(["{:.0f}%".format(y) for y in ax.get_yticks()])

data = [batch_mem, colocted_mem, online_mem]

plt.boxplot(data, flierprops={"marker" :'_'}, whis=[5, 95], labels=["Batch Only", "Co-located", "Online Only"])
plt.grid(linestyle='dotted')
plt.show()

###
# Plot mpki
colocted_mpki = np.reshape(colocated.select("mpki").dropna().collect(), -1)
batch_mpki = np.reshape(batch.select("mpki").dropna().collect(), -1)
online_mpki = np.reshape(online.select("mpki").dropna().collect(), -1)

data = [batch_mpki, colocted_mpki, online_mpki]
print(data)

plt.boxplot(data, flierprops={"marker" :'_'}, labels=["Batch Only", "Co-located", "Online Only"])
plt.grid(linestyle='dotted')
plt.show()
