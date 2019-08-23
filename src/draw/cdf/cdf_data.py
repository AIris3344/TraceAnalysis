from pyspark.sql import SparkSession

data_path = "hdfs://10.1.4.11:9000/user/hduser/"

spark = SparkSession.builder     .master("local[*]")     .appName("TraceAnalysis")     .config("spark.driver.memory", "8g")     .getOrCreate()

# Read batck_task parquet
df_batch_task = spark    .read    .parquet(data_path + "batch_task_parquet")
df_batch_task.createOrReplaceTempView("batch_task")

# Read batch_instance parquet
df_batch_instance = spark    .read    .parquet(data_path + "batch_instance_parquet")
df_batch_instance.createOrReplaceTempView("batch_instance")

######################## Write staging results to HDFS ####################################

# # Write job total time to staging results
df_batch_task = spark.sql("SELECT SUM(end_time - start_time) AS duration FROM batch_task GROUP BY job_name")
df_batch_task.write.text(data_path + "batch_task_staging/job_duration.txt")
# 
# # Write task total time to staging results
df_batch_task = spark.sql("SELECT SUM(end_time - start_time) AS duration FROM batch_instance GROUP BY task_name")
df_batch_task.write.parquet(data_path + "batch_instance_staging/task_duration.txt")
# 
# # Write instance total time to staging results
df_batch_instance = spark.sql("SELECT SUM(end_time - start_time) AS duration FROM batch_instance GROUP BY instance_name")
df_batch_instance.write.parquet(data_path + "batch_instance_staging/instance_duration.txt")

###########################################################################################