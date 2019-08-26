#%%
import os
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

ins = spark.read.parquet(write_path + "batch_instance_staging/instance_duration").filter("duration >= 0").orderBy("duration").createOrReplaceTempView("ins_duration")
ins = spark.sql("SELECT group_data, AVG(duration) AS duration FROM (SELECT duration, FLOOR(duration / 10) AS group_data FROM ins_duration) t GROUP BY group_data ORDER BY group_data")
ins.select("duration").coalesce(1).write.csv(write_path + "batch_instance_staging/ins_reduce_csv")
