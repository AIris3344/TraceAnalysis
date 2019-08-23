import matplotlib.pylab as plt
import numpy as np
from matplotlib.ticker import ScalarFormatter
import matplotlib.pyplot as plt
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from matplotlib.ticker import ScalarFormatter
import statsmodels.api as sm



import scipy
import matplotlib.pyplot as plt
import seaborn as sns

x = np.random.randn(10000) # generate samples from normal distribution (discrete data)
norm_cdf = scipy.stats.norm.cdf(x) # calculate the cdf - also discrete

# plot the cdf
sns.lineplot(x=x, y=norm_cdf)
plt.show()

"""
x = [100, 1000, 100000, 10000, 1000000]
y = [1, 2, 3, 10, 100]


plt.plot(x, y)
plt.xscale('log')
plt.grid()
left, right = plt.xlim()
print(left, right)
plt.xlim(left=0.5)
ax = plt.gca()

plt.show()



data = np.random.rand(100)

ecdf = sm.distributions.ECDF(data)

x = np.linspace(min(data), max(data))
y = ecdf(x)



print(x)

print(y)

y = np.array([0.97, 0.98])

number = np.argwhere(y == 0.97)[0][0]
print(number)
"""
