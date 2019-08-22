import matplotlib.pylab as plt
import numpy as np
from matplotlib.ticker import ScalarFormatter

x = 10 ** np.arange(0, 5, 1)
y = [1,2,3,10,100]


plt.plot(x, y)
plt.xscale('log')
plt.grid()

ax = plt.gca()
ax.set_xticks(x[1:]) # note that with a log axis, you can't have x = 0 so that value isn't plotted.

plt.show()