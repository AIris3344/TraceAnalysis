# To add a new cell, type '#%%'
# To add a new markdown cell, type '#%% [markdown]'
#%%
import matplotlib.pyplot as plt
import numpy as np
import statsmodels.api as sm
import os
import seaborn as sns
import pandas as pd
from matplotlib import mlab
from pyspark.sql import SparkSession
from matplotlib.ticker import ScalarFormatter
from scipy.stats import norm
from more_itertools import chunked

# data_path = "hdfs://10.1.4.11:9000/user/hduser/"
# local_path = os.getcwd() + "/data/"
data_path = os.environ['HOME'] + "/data/"

spark = SparkSession.builder.master("local[*]").appName("TraceAnalysis").config("spark.driver.memory", "11g").getOrCreate()

#%%
# Tidy up figure
plt.figure(figsize=[11, 4])
plt.xscale('log')
plt.xlabel("Duration(seconds)")
plt.ylabel("CDF")
plt.xlim(0.5, 10 ** 6 + 100)
plt.ylim(0, 1.01)

# Read staging results
data = np.loadtxt(data_path + "batch_task_staging/job_duration_csv/job.csv")

#%%
# Compute ecdf and values of x and y
ecdf = sm.distributions.ECDF(data)
x = np.linspace(min(data), max(data), num=8000000)
y = ecdf(x)

# Get the first index of x when y = 0.99
x_99 = np.argwhere(y >= 0.99)[0][0]

# Plot job
plt.plot(x, y, label="job", color="r")
plt.vlines(x_99, 0, 0.99, colors="r", linestyles="dashed", label="99% job")
plt.text(x_99, 0.99, "", size=10, position=(x_99, 0.5), fontdict={"color": "r"})

#%%
data = np.loadtxt(data_path + "batch_task_staging/task_duration_csv/task.csv")

#%%
# Compute ecdf and values of x and y
ecdf = sm.distributions.ECDF(data)
x = np.linspace(np.amin(data), np.amax(data), num=600000)
y = ecdf(x)

# Get the first index of x when y = 0.99
x_99 = np.argwhere(y >= 0.99)[0][0]

plt.plot(x, y, label="task", color="g")
plt.vlines(x_99, 0, 0.99, colors="g", linestyles="dashed", label="99% task")
plt.text(x_99, 0.99, "", size=10, position=(x_99, 0.5), fontdict={"color": "g"})

#%%
data = np.genfromtxt(data_path + "batch_instance_staging/ins_csv/average.csv")

#%%
# Compute ecdf and values of x and y
# There are more than 10 billion, sort and take 100 average to reduce size
ecdf = sm.distributions.ECDF(data)
x = np.linspace(min(data), max(data), num=375000)
y = ecdf(x)

# Get the first index of x when y = 0.99
x_99 = np.argwhere(y >= 0.99)[0][0]

# Plot job
plt.plot(x, y, label="instance", color="b")
plt.vlines(x_99, 0, 0.99, colors="b", linestyles="dashed", label="99% instance")
plt.text(x_99, 0.99, "", size=10, position=(x_99, 0.5), fontdict={"color": "b"})

#%%
ax = plt.gca()
ax.set_yticklabels(["{:.0f}%".format(y * 100) for y in ax.get_yticks()])
plt.legend(loc='center right')

# Plot
plt.grid()
plt.show()