## Thif file is to draw different patterns of CPI and CPU in cluster
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

meta = spark.read.parquet(data_path + "container_meta_parquet")
# meta.show()
meta.createOrReplaceTempView("meta")

usage = spark.read.parquet(data_path + "container_usage_parquet")
# usage.show()
usage.createOrReplaceTempView("usage")

# Machine
machine_usage = spark.sql("SELECT machine_id, AVG(cpu_util_percent) AS cpu, AVG(mem_util_percent) AS mem, AVG(cpi) AS cpi, AVG(mpki) AS mpki FROM (SELECT CAST(SUBSTRING(machine_id, 3, 20) AS INT) AS machine_id, cpu_util_percent, mem_util_percent, cpi, mpki FROM usage) t GROUP BY machine_id ORDER BY machine_id")
machine_usage.write.parquet(write_path + "usage_machine_parquet")

# CPU
meta = spark.sql("SELECT container_id, AVG(cpu_request) / 100 AS cpu_request, AVG(cpu_limit) / 100 AS cpu_limit FROM (SELECT CAST(SUBSTRING(container_id, 3, 20) AS INT) AS container_id, cpu_request, cpu_limit FROM meta) t GROUP BY container_id ORDER BY container_id")
meta.show()
meta.createOrReplaceTempView("meta")

result = spark.sql("SELECT t1.container_id, (t1.cpu * t2.cpu_request) / 100 AS cpu, t2.cpu_request, t2.cpu_limit FROM usage t1 JOIN meta t2 ON t1.container_id = t2.container_id ORDER BY t1.container_id")
result.show()
x = np.reshape(result.select("container_id").collect(), -1)

fig, ax = plt.subplots(figsize=(10, 5))
ax.set_ylabel("CPU")

ax.plot(x, np.reshape(result.select("cpu").collect(), -1), color="r", label="CPU utilization")
ax.plot(x, np.reshape(result.select("cpu_request").collect(), -1), color="g", label="CPU request")
ax.plot(x, np.reshape(result.select("cpu_limit").collect(), -1), color="gray", label="CPU limit")

plt.legend()
plt.show()

#%%
## Container
# usage = spark.sql("SELECT container_id, AVG(cpu_util_percent) AS cpu, AVG(mem_util_percent) AS mem, AVG(cpi) AS cpi, AVG(mpki) AS mpki FROM (SELECT CAST(SUBSTRING(container_id, 3, 20) AS INT) AS container_id, cpu_util_percent, mem_util_percent, cpi, mpki FROM usage) t GROUP BY container_id ORDER BY container_id")
# usage.write.parquet(write_path + "usage_parquet")

usage = spark.read.parquet(write_path + "usage_parquet")
usage.show()
usage.createOrReplaceTempView("usage")

meta = spark.sql("SELECT CAST(SUBSTRING(container_id, 3, 20) AS INT) AS container_id, cpu_request FROM meta GROUP BY container_id, cpu_request")
meta.show()
meta.createOrReplaceTempView("meta")

result = spark.sql("SELECT t1.container_id, t1.cpu, t1.cpi, t2.cpu_request FROM usage t1 JOIN meta t2 ON t1.container_id = t2.container_id ORDER BY t1.container_id")
result.show()

#%%
result.createOrReplaceTempView("result")
cpu_result = spark.sql("SELECT AVG(cpu) AS cpu, AVG(cpi) AS cpi, cpu_request FROM result GROUP BY cpu_request ORDER BY cpu_request")
cpu_result.show()

#%%
fig, axes = plt.subplots(1, 2, sharex=True, figsize=(10, 5))
ax = axes[0]
ax.set_ylabel("CPU")
ax.set_ylim(0, 20)
ax.set_yticklabels(["{:.1f}%".format(y) for y in ax.get_yticks()])
name_list = np.reshape(cpu_result.select("cpu_request").collect(), -1)
values = np.reshape(cpu_result.select("cpu").collect(), -1)
ax.bar(range(len(values)), values, tick_label=name_list)

ax = axes[1]
ax.set_ylabel("CPI")
values = np.reshape(cpu_result.select("cpi").collect(), -1)
ax.bar(range(len(values)), values, tick_label=name_list)

plt.show()
