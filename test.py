import csv
import pandas as pd
import numpy as np
import time
import sys
import random
from pandas import DataFrame

datenow=(time.strftime("%Y-%m-%d"))
filename=sys.argv[1]

#df = pd.read_csv(filename)
df = pd.read_csv(filename, usecols=['EMP_NO','USERNAME','FIRST_NAME','LAST_NAME','Email','AD','EXCHANGE','MANAGER','POSITION','WORK_LOCATION'], sep=';')
#df = pd.read_csv(filename)
#df.loc[(df['AD']=="YES") & (df['EXCHANGE']=="YES"),['EMP_NO','USERNAME','Email']]
#df = pd.read_csv(filename, sep=';')
#df = pd.read_csv(filename,names =['EMP_NO','USERNAME','Email','AD','EXCHANGE','MANAGER'], sep=';' )
#df = pd.read_csv(filename,names =['MANAGER'], sep=';' )

#data['EMP_NO'] = [random.randint(0,1000) for x in range(data.shape[0])]
#df = pd.DataFrame(data,)

#Replace header colom 
#df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(', '').str.replace(')', '')
 
#data.head(5)
#print(df)
#print(df["EMP_NO"],df["MANAGER"].astype('str').replace('\.0', '', regex=True))
ADEXCHANGE = (df["AD"]=="YES") & (df['EXCHANGE']=="YES")
#df.loc[df.AD == "YES"]
#print(pd.notnull(df["MANAGER"]))
#df.head(4)
print(df)
