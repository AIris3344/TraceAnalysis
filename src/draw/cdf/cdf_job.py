# To add a new cell, type '#%%'
# To add a new markdown cell, type '#%% [markdown]'
#%%
import matplotlib.pyplot as plt
import numpy as np
from pyspark.sql import SparkSession
from matplotlib.ticker import ScalarFormatter
import statsmodels.api as sm
import os

data_path = "hdfs://10.1.4.11:9000/user/hduser/"
local_path = os.environ['HOME'] + "/data/"

spark = SparkSession.builder     .master("local[*]")     .appName("TraceAnalysis")     .config("spark.driver.memory", "10g")     .getOrCreate()

#%%
# Read staging results
duration = spark.read.parquet(local_path + "batch_task_staging/job_duration_parquet").filter("duration >= 0")

# Reshape the list to 1-d array
rows = duration.count()
data = np.reshape(duration.select("duration").collect(), rows)

# Compute ecdf and values of x and y
ecdf = sm.distributions.ECDF(data)
x = np.linspace(min(data), max(data), num=rows)
y = ecdf(x)

# Get the first index of x when y = 0.99
x_99 = np.argwhere(y >= 0.99)[0][0]

# Plot job
plt.plot(x, y, label="job")
plt.vlines(x_99, 0, 0.99, colors="r", linestyles="dashed", label="99% job")
plt.text(x_99, 0.99, str(x_99), size=10, position=(x_99, 0.5))

#%%
duration = spark.read.parquet(data_path + "batch_instance_staging/task_duration_parquet").filter("duration >= 0")

# Reshape the list to 1-d array
rows = duration.count()
data = np.reshape(duration.select("duration").collect(), rows)

# Compute ecdf and values of x and y
ecdf = sm.distributions.ECDF(data)
x = np.linspace(min(data), max(data), num=rows)
y = ecdf(x)

# Get the first index of x when y = 0.99
x_99 = np.argwhere(y >= 0.99)[0][0]

# Plot job
plt.plot(x, y, label="task")
plt.vlines(x_99, 0, 0.99, colors="r", linestyles="dashed", label="99% task")
plt.text(x_99, 0.99, str(x_99), size=10, position=(x_99, 0.5))

#%%
duration = spark.read.parquet(local_path + "batch_instance_staging/instance_duration_parquet").filter("duration >= 0")
#%%
# Reshape the list to 1-d array
rows = duration.count()
data = np.reshape(duration.select("duration").collect(), rows)

# Compute ecdf and values of x and y
ecdf = sm.distributions.ECDF(data)
x = np.linspace(min(data), max(data), num=rows)
y = ecdf(x)

# Get the first index of x when y = 0.99
x_99 = np.argwhere(y >= 0.99)[0][0]

# Plot job
plt.plot(x, y, label="instance")
plt.vlines(x_99, 0, 0.99, colors="r", linestyles="dashed", label="99% instance")
plt.text(x_99, 0.99, str(x_99), size=10, position=(x_99, 0.5))

#%%
# Tidy up figure
plt.figure(figsize=[11, 4])
plt.xscale('log')
plt.xlabel("Duration(seconds)")
plt.ylabel("CDF")
plt.xlim(0.5, 10 ** 6 + 100)
plt.ylim(0, 1.01)
plt.legend(loc='center right')
ax = plt.gca()
ax.set_yticklabels(["{:.0f}%".format(y * 100) for y in ax.get_yticks()])
# Plot
plt.grid()
plt.show()
