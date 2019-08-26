#%%
import os
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession

data_path = "hdfs://10.1.4.11:9000/user/hduser/"
write_path = os.environ['HOME'] + "/data/"

spark = SparkSession.builder.master("local[*]").appName("TraceAnalysis").config("spark.driver.memory", "8g").getOrCreate()

# Read batck_task parquet
df_batch_task = spark.read.parquet(data_path + "batch_task_parquet")
df_batch_task.createOrReplaceTempView("batch_task")
df_batch_task.show()

# Read batch_instance parquet
df_batch_instance = spark.read.parquet(data_path + "batch_instance_parquet")
df_batch_instance.createOrReplaceTempView("batch_instance")
df_batch_instance.show()

#%%
######################## Write staging results to HDFS ####################################

# # Write job total time to staging results
df_batch_task = spark.sql("SELECT CAST(SUM(end_time - start_time) AS INT) AS duration FROM batch_task WHERE status='Terminated' GROUP BY job_name")
df_batch_task.write.parquet(write_path + "batch_task_staging/job_duration")

# # Write task total time to staging results
df_batch_task = spark.sql("SELECT CAST(SUM(end_time - start_time) AS INT) AS duration FROM batch_task WHERE status='Terminated' GROUP BY job_name, task_name")
df_batch_task.write.parquet(write_path + "batch_task_staging/task_duration")

# 
# # Write instance total time to staging results
df_batch_instance = spark.sql("SELECT CAST(SUM(end_time - start_time) AS INT) AS duration FROM batch_instance GROUP BY instance_name ORDER BY duration")
df_batch_instance.write.parquet(write_path + "batch_instance_staging/ins_duration")

###########################################################################################
job = spark.read.parquet(write_path + "batch_task_staging/job_duration").filter("duration >= 0").orderBy("duration")
job.coalesce(1).write.csv(write_path + "batch_task_staging/job_duration_csv")

task = spark.read.parquet(write_path + "batch_task_staging/task_duration").filter("duration >= 0").orderBy("duration")
task.coalesce(1).write.csv(write_path + "batch_task_staging/task_duration_csv")

ins = spark.read.parquet(write_path + "batch_instance_staging/instance_duration").filter("duration >= 0").orderBy("duration")
ins.coalesce(1).write.csv(write_path + "batch_instance_staging/ins_reduce_csv")

# The data set is too big just split, 1347372775 lines
data = pd.Series([])

chunkSize = 1000000
reader = pd.read_csv(write_path + "batch_instance_staging/ins_csv/ins.csv", iterator=True, dtype=np.uint32)
loop = True
while loop:
    try:
        df = reader.get_chunk(chunkSize)
        for i in np.arange(0, chunkSize, 100):
            data = data.append(df.iloc[i : i + 100].mean())
        data.to_csv(data_path + "batch_instance_staging/ins_csv/average.csv", mode="a+", index=False, header=False)
        data = pd.Series([])
    except StopIteration:
        loop = False
        print("Iteration is stopped.")