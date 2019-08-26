import matplotlib.pylab as plt
import numpy as np
from matplotlib.ticker import ScalarFormatter
from pyspark.sql import SparkSession
import statsmodels.api as sm


import numpy as np
import matplotlib.pyplot as plt

# ax = plt.subplot(111)
t1 = np.arange(0.0, 1.0, 0.01)
plt.plot(t1, t1**1, label="n=%d"%(1,))


t1 = np.arange(1.0, 2.0, 0.01)
plt.plot(t1, t1**2, label="n=%d"%(2,))

leg = plt.legend(loc='best', ncol=2, mode="expand", shadow=True, fancybox=True)

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
