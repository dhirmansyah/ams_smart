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
df = pd.read_csv(filename, usecols=['EMP_NO','USERNAME','FIRST_NAME','LAST_NAME','Email','AD','EXCHANGE','MANAGER','POSITION','WORK_LOCATION','SUPERVISOR'], sep=',|;', engine='python')



# Spesify Field by feature and Position
adexchange = df.loc[(df.AD == 'YES') & (df.EXCHANGE == 'YES')]
adzimbra = df.loc[(df.AD == 'YES') & (df.EXCHANGE == 'NO')]
zimbraonly = df.loc[(df.AD == 'NO') & (df.EXCHANGE == 'NO')]


# Printing based on value
print ("-----------------------Exchange only--------------------------------")
print(adexchange.astype('str').replace('\.0', '', regex=True))
print ("-----------------------AD Zimbra--------------------------------")
print(adzimbra.astype('str').replace('\.0', '', regex=True))
print ("-----------------------Zimbra only--------------------------------")
print(zimbraonly.astype('str').replace('\.0', '', regex=True))
