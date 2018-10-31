import csv
import pandas as pd
import numpy as np
import time
import sys
import random
from pandas import DataFrame

datenow=(time.strftime("%Y-%m-%d"))
filename=sys.argv[1]
ADEX_FILE='/opt/output/adexchange.csv'

#df = pd.read_csv(filename)
df = pd.read_csv(filename, usecols=['EMP_NO','USERNAME','FIRST_NAME','LAST_NAME','Email','AD','EXCHANGE','MANAGER','POSITION','WORK_LOCATION','SUPERVISOR','PASSWORD'], sep=',|;', engine='python')



# Spesify Field by feature and Position
adexchange = df.loc[(df.AD == 'YES') & (df.EXCHANGE == 'YES')]
adzimbra = df.loc[(df.AD == 'YES') & (df.EXCHANGE == 'NO')]
zimbraonly = df.loc[(df.AD == 'NO') & (df.EXCHANGE == 'NO')]


# Printing based on value 1 before export
print ("-----------------------Exchange only--------------------------------")
print(adexchange.astype('str').replace('\.0', '', regex=True))
print ("-----------------------AD Zimbra--------------------------------")
print(adzimbra.astype('str').replace('\.0', '', regex=True))
print ("-----------------------Zimbra only--------------------------------")
print(zimbraonly.astype('str').replace('\.0', '', regex=True))

# Export to csv ADEX
exportadexchange = adexchange.astype('str').replace('\.0', '', regex=True)
tbl_adex=['employe_number','employe_number','employe_number','employe_number','employe_number','employe_number','employe_number','employe_number','employe_number']
exportadexchange.to_csv(ADEX_FILE, sep=',', encoding='utf-8', index = None, header=tbl_adex,columns=['EMP_NO','USERNAME','FIRST_NAME','LAST_NAME','Email','MANAGER','POSITION','WORK_LOCATION','SUPERVISOR'])
