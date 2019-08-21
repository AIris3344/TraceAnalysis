import matplotlib.pyplot as plt
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from matplotlib.ticker import FormatStrFormatter
from scipy.stats import norm
import seaborn as sns
from matplotlib.ticker import ScalarFormatter
import statsmodels.api as sm

data_path = "hdfs://10.1.4.11:9000/user/hduser/"

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("TraceAnalysis") \
    .config("spark.driver.memory", "8g") \
    .getOrCreate()

#
schema_container_meta = StructType([\
    StructField("container_id", StringType(), True),\
    StructField("machine_id", StringType(), True),\
    StructField("time_stamp", LongType(), True),\
    StructField("app_du", StringType(), True),\
    StructField("status", StringType(), True),\
    StructField("cpu_request", LongType(), True),\
    StructField("cpu_limit", LongType(), True),\
    StructField("mem_size", DoubleType(), True)])

df_container_meta = spark\
    .read\
    .csv(data_path + "container_meta.csv", header=False, schema=schema_container_meta)
print("container_mata:")
df_container_meta.show()

df_container_meta = df_container_meta\
    .select("container_id", "app_du")\
    .dropDuplicates()\
    .groupBy("app_du")\
    .agg({"app_du": "count"})

df_container_meta.show()

# Reshape the list to 1-d array
rows = df_container_meta.count()
data = np.reshape(df_container_meta.select("count(app_du)").collect(), rows)

# Compute ecdf and values of x and y
ecdf = sm.distributions.ECDF(data)
x = np.linspace(min(data), max(data))
y = ecdf(x)

# Set layout
plt.figure(figsize=[11, 4])
plt.xscale('log')
plt.xlabel("Container")
plt.ylabel("CDF")
plt.xlim(1, 600)
plt.ylim(0, 1.01)
plt.xticks(2 ** np.arange(10))
plt.yticks(np.arange(0, 1.2, 0.2))

ax = plt.gca()
ax.xaxis.set_major_formatter(ScalarFormatter())
ax.set_yticklabels(["{:.0f}%".format(y * 100) for y in ax.get_yticks()])

# Plot
plt.plot(x, y, label="service")
plt.vlines(101, 0, 0.99, colors="r", linestyles="dashed", label="99% service")
plt.text(101, 0.99, "101", size=18, position=(101, 0.5))
plt.legend(loc='upper right')
plt.grid()
plt.show()