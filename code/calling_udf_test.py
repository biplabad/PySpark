import pandas as pd
import numpy as np
#from udf import bool_test
def bool_test(num):
    return bool(num%1000 == 0)
df = pd.DataFrame([{'c1':10000}, {'c1':112345}, {'c1':1200}])

for index, row in df.iterrows():
    x = row['c1']
    y = bool_test(x)
    print(row['c1'],y)