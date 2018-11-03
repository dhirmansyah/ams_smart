import pandas as pd
import numpy as np
import time
import sys
import random

datenow=(time.strftime("%Y-%m-%d"))
filename=sys.argv[1]

df = pd.read_csv(filename)
#df = pd.read_csv(filename, usecols=['EMP_NO','USERNAME','Email','AD','EXCHANGE'], sep=',')
#df = pd.read_csv(filename)
#df.loc[(df['AD']=="YES") & (df['EXCHANGE']=="YES"),['EMP_NO','USERNAME','Email']]

#data['EMP_NO'] = [random.randint(0,1000) for x in range(data.shape[0])]
#df = pd.DataFrame(data,)

#Replace header colom 
#df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(', '').str.replace(')', '')
 
#data.head(5)
df.sample(2)
print(df.sample)
