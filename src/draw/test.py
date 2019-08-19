import seaborn as sns, numpy as np
import matplotlib.pyplot as plt
sns.set()
np.random.seed(1)
x = np.random.uniform(0.8, 0.95, 100)
print(x)
ax = sns.distplot(x)
plt.show()

import pandas as pd

x = pd.Series(x, name="ssss")
print(x)
ax = sns.distplot(x)
plt.show()