import matplotlib.pylab as plt
import numpy as np
from matplotlib.ticker import ScalarFormatter

x = [0, 10, 20, 50, 100]
y = [1,2,3,10,100]

plt.plot(x, y)
plt.xscale('log')
plt.grid()

ax = plt.gca()
ax.set_xticks(x[1:]) # note that with a log axis, you can't have x = 0 so that value isn't plotted.
ax.xaxis.set_major_formatter(ScalarFormatter())

plt.show()