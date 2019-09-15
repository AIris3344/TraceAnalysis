#%%
import os
import numpy as np
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession

data_path = "hdfs://10.1.4.11:9000/user/hduser/"
write_path = os.environ['HOME'] + "/data/"

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Performance_Machine") \
    .getOrCreate()
"""
#%%
# Write data
machine_usage = spark.read.parquet(write_path + "machine_usage/staging")
machine_usage.show()

machine_usage = machine_usage.groupBy("machine_id", "t_hour").agg({"mpki": "avg", "mem": "avg", "cpu": "avg"})
machine_usage = machine_usage \
    .select("machine_id", "t_hour", machine_usage["avg(mpki)"].alias("mpki"), machine_usage["avg(mem)"].alias("mem"), machine_usage["avg(cpu)"].alias("cpu"))

machine_usage.write.parquet(write_path + "machine_usage/performance_day")

#%%
machine = spark.read.parquet(write_path + "machine_usage/performance_day")
machine.show()

# Overall data
all_machines = machine.groupBy("t_hour") \
    .agg({"mem": "avg", "cpu": "avg", "mpki": "avg"}) \

all_machines = all_machines.select( \
        (all_machines["t_hour"] / 24).alias("t_hour"), \
        all_machines["avg(mem)"].alias("mem"), \
        all_machines["avg(cpu)"].alias("cpu"), \
        all_machines["avg(mpki)"].alias("mpki"), \
    )
all_machines.show()
all_machines.write.parquet(write_path + "machine_usage/performance_all_machines")

# Batch only data
data_list = np.genfromtxt(write_path + "batch_only.csv", dtype='str')
data = machine.filter(machine["machine_id"].isin(data_list.tolist()))

data = data.groupBy("t_hour") \
    .agg({"cpu": "avg", "mem": "avg", "mpki": "avg"}) \

data = data.select( \
        (data["t_hour"] / 24).alias("t_hour"), \
        data["avg(cpu)"].alias("cpu"), \
        data["avg(mem)"].alias("mem"), \
        data["avg(mpki)"].alias("mpki"), \
    )
data.show()
data.write.parquet(write_path + "machine_usage/performance_batch_only")


# Colocated data
data_list = np.genfromtxt(write_path + "colocated.csv", dtype='str')
data = machine.filter(machine["machine_id"].isin(data_list.tolist()))

data = data.groupBy("t_hour") \
    .agg({"cpu": "avg", "mem": "avg", "mpki": "avg"}) \

data = data.select( \
        (data["t_hour"] / 24).alias("t_hour"), \
        data["avg(cpu)"].alias("cpu"), \
        data["avg(mem)"].alias("mem"), \
        data["avg(mpki)"].alias("mpki"), \
    )
data.write.parquet(write_path + "machine_usage/performance_colocated")

# Online only data
data_list = np.genfromtxt(write_path + "online_only.csv", dtype='str')
data = machine.filter(machine["machine_id"].isin(data_list.tolist()))

data = data.groupBy("t_hour") \
    .agg({"cpu": "avg", "mem": "avg", "mpki": "avg"}) \

data = data.select( \
        (data["t_hour"] / 24).alias("t_hour"), \
        data["avg(cpu)"].alias("cpu"), \
        data["avg(mem)"].alias("mem"), \
        data["avg(mpki)"].alias("mpki"), \
    )
data.write.parquet(write_path + "machine_usage/performance_online_only")
"""
#%%
data = spark.read.parquet(write_path + "machine_usage/performance_all_machines").orderBy("t_hour")
## Plot all machines CPU and Memory 
fig, ax = plt.subplots()

x = np.reshape(data.select("t_hour").collect(), -1)
y = np.reshape(data.select("cpu").collect(), -1)

ax.plot(x, y, label="CPU")

x = np.reshape(data.select("t_hour").collect(), -1)
y = np.reshape(data.select("mem").collect(), -1)
ax.plot(x, y, label="Memory")

ax.set_ylabel("Utilization")
ax.set_xlabel("Time(days)")
ax.set_xlim(0, 9)
ax.set_ylim(0, 100)
ax.set_xticks(np.arange(0, 10, 1))
ax.set_yticks(np.arange(0, 110, 10))
ax.set_yticklabels(["{:.1f}%".format(y) for y in ax.get_yticks()])
ax.legend(ncol=2, bbox_to_anchor=(0.3, 1), loc='lower left', fontsize='small')

plt.grid(linestyle='dotted')
plt.show()

## Plot MPKI
fig, ax = plt.subplots()
data = data.select("t_hour", "mpki").fillna(0)

x = np.reshape(data.select("t_hour").collect(), -1)
y = np.reshape(data.select("mpki").collect(), -1) / 1000

ax.plot(x, y, label="MPKI")

ax.set_xlabel("Time(days)")
ax.set_ylabel("Cache miss / Instruction")
ax.set_xlim(0, 9)

plt.grid(linestyle='dotted')
plt.show()


#%%
######################################################################
data = spark.read.parquet(write_path + "machine_usage/performance_batch_only").orderBy("t_hour")

## Plot batch only machines CPU and Memory 
fig, ax = plt.subplots()

x = np.reshape(data.select("t_hour").collect(), -1)
y = np.reshape(data.select("cpu").collect(), -1)

ax.plot(x, y, label="CPU")

x = np.reshape(data.select("t_hour").collect(), -1)
y = np.reshape(data.select("mem").collect(), -1)
ax.plot(x, y, label="Memory")

ax.set_ylabel("Utilization")
ax.set_xlabel("Time(days)")
ax.set_xlim(0, 9)
ax.set_ylim(0, 100)
ax.set_xticks(np.arange(0, 10, 1))
ax.set_yticks(np.arange(0, 110, 10))
ax.set_yticklabels(["{:.1f}%".format(y) for y in ax.get_yticks()])
ax.legend(ncol=2, bbox_to_anchor=(0.3, 1), loc='lower left', fontsize='small')

plt.grid(linestyle='dotted')
plt.show()

## Plot MPKI
fig, ax = plt.subplots()
data = data.select("t_hour", "mpki").fillna(0)

x = np.reshape(data.select("t_hour").collect(), -1)
y = np.reshape(data.select("mpki").collect(), -1) / 1000

ax.plot(x, y, label="MPKI")

ax.set_xlabel("Time(days)")
ax.set_ylabel("Cache miss / Instruction")
ax.set_xlim(0, 9)

plt.grid(linestyle='dotted')
plt.show()

#%%
###########################################
data = spark.read.parquet(write_path + "machine_usage/performance_colocated").orderBy("t_hour")

## Plot colocated machines CPU and Memory 
fig, ax = plt.subplots()

x = np.reshape(data.select("t_hour").collect(), -1)
y = np.reshape(data.select("cpu").collect(), -1)

ax.plot(x, y, label="CPU")

x = np.reshape(data.select("t_hour").collect(), -1)
y = np.reshape(data.select("mem").collect(), -1)
ax.plot(x, y, label="Memory")

ax.set_ylabel("Utilization")
ax.set_xlabel("Time(days)")
ax.set_xlim(0, 9)
ax.set_ylim(0, 100)
ax.set_xticks(np.arange(0, 10, 1))
ax.set_yticks(np.arange(0, 110, 10))
ax.set_yticklabels(["{:.1f}%".format(y) for y in ax.get_yticks()])
ax.legend(ncol=2, bbox_to_anchor=(0.3, 1), loc='lower left', fontsize='small')

plt.grid(linestyle='dotted')
plt.show()


#%%
## Plot MPKI
fig, ax = plt.subplots()
data = data.select("t_hour", "mpki").fillna(0)

x = np.reshape(data.select("t_hour").collect(), -1)
y = np.reshape(data.select("mpki").collect(), -1) / 1000

ax.plot(x, y, label="MPKI")

ax.set_xlabel("Time(days)")
ax.set_ylabel("Cache miss / Instruction")
ax.set_xlim(0, 9)

plt.grid(linestyle='dotted')
plt.show()

#%%
####################################################
data = spark.read.parquet(write_path + "machine_usage/performance_online_only").orderBy("t_hour")

## Plot online only machines CPU and Memory 
fig, ax = plt.subplots()

x = np.reshape(data.select("t_hour").collect(), -1)
y = np.reshape(data.select("cpu").collect(), -1)

ax.plot(x, y, label="CPU")

x = np.reshape(data.select("t_hour").collect(), -1)
y = np.reshape(data.select("mem").collect(), -1)
ax.plot(x, y, label="Memory")

ax.set_ylabel("Utilization")
ax.set_xlabel("Time(days)")
ax.set_xlim(0, 9)
ax.set_ylim(0, 100)
ax.set_xticks(np.arange(0, 10, 1))
ax.set_yticks(np.arange(0, 110, 10))
ax.set_yticklabels(["{:.1f}%".format(y) for y in ax.get_yticks()])
ax.legend(ncol=2, bbox_to_anchor=(0.3, 1), loc='lower left', fontsize='small')

plt.grid(linestyle='dotted')
plt.show()

## Plot MPKI
fig, ax = plt.subplots()
data = data.select("t_hour", "mpki").fillna(0)

x = np.reshape(data.select("t_hour").collect(), -1)
y = np.reshape(data.select("mpki").collect(), -1) / 1000

ax.plot(x, y, label="MPKI")

ax.set_xlabel("Time(days)")
ax.set_ylabel("Cache miss / Instruction")
ax.set_xlim(0, 9)

plt.grid(linestyle='dotted')
plt.show()
#####################################