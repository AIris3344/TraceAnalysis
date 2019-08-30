#%% Calculate the three regions: batch_only, online_only, colocated
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import *

data_path = "hdfs://10.1.4.11:9000/user/hduser/"
write_path = os.environ['HOME'] + "/data/"

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("ConvertParquet") \
    .getOrCreate()

#%%
df_machine_meta = spark.read.parquet(data_path + "machine_meta_parquet")
print("machine_meta:")
df_machine_meta.show()
df_machine_meta.createOrReplaceTempView("machine_meta")

df_container_meta = spark.read.parquet(data_path + "container_meta_parquet")
print("container_meta:")
df_container_meta.show()
df_container_meta.createOrReplaceTempView("container_meta")

df_batch_instance = spark.read.parquet(data_path + "batch_instance_parquet")
print("batch_instance:")
df_batch_instance.show()

df_container_usage = spark.read.parquet(data_path + "container_usage_parquet")
print("container_usage:")
df_container_usage.show()

#%%
all_   = df_machine_meta.select("machine_id").groupBy("machine_id").count().select("machine_id")
batch  = df_batch_instance.select("machine_id").groupBy("machine_id").count().select("machine_id")
online = df_container_meta.select("machine_id").groupBy("machine_id").count().select("machine_id")
# online = df_container_usage.where(df_container_usage["container_id"].isNotNull()).groupBy("machine_id").count().select("machine_id")

all_.write.parquet(write_path + "machines_group/all")
batch.write.parquet(write_path + "machines_group/batch")
online.write.parquet(write_path + "machines_group/online")
# online_bak.write.parquet(write_path + "machines_group/online_bak")


#%%
all_   = spark.read.parquet(write_path + "machines_group/all")
online = spark.read.parquet(write_path + "machines_group/online")
batch  = spark.read.parquet(write_path + "machines_group/batch")
# online_bak = spark.read.parquet(write_path + "machines_group/online_bak")

# #%% 4015
# machines = online_bak.unionAll(online).unionAll(batch).groupBy("machine_id").count().select("machine_id")
# print(machines.count())

# #%%
# # 16台有问题机器，都没有container, 其中有4台跑了batch,可以算到纯batch的
# error_ids = online_bak.subtract(online_bak.intersect(online))
# print(machines.intersect(error_ids).count())

# #%%
# used_in_error_ids = batch.intersect(error_ids)
# used_in_error_ids.show()
# print(used_in_error_ids.count())
# ids_except = error_ids.subtract(used_in_error_ids)
# print(ids_except.count())
# batch = batch.union(used_in_error_ids)

#%%
# 3853
colocated = online.intersect(batch)
colocated.coalesce(1).write.csv(write_path + "machines_group/colocated.csv")
print(colocated.count())
# 136
online_only = online.subtract(batch)
online_only.coalesce(1).write.csv(write_path + "machines_group/online_only.csv")
print(online_only.count())
# 14
batch_only = batch.subtract(online)
batch_only.coalesce(1).write.csv(write_path + "machines_group/batch_only.csv")
print(batch_only.count())
# 4003
machines = all_
machines.coalesce(1).write.csv(write_path + "machines_group/all_machines.csv")
print(machines.count())