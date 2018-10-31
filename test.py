import csv
import pandas as pd
import numpy as np
import time
import sys
import random
from pandas import DataFrame


# Golbal Variable
datenow=(time.strftime("%Y-%m-%d"))
COS_LIST='/opt/automateams/var/cos_list.csv'
DL_LIST='/opt/automateams/var/dl_list.csv'
filename=sys.argv[1]
ADEX_FILE='/opt/output/adexchange.csv'
ZM_FILE='/opt/output/adzimbra.csv'

# Index COS file
cos_list = pd.read_csv(COS_LIST,sep=',|;', engine='python')

# Index DL file
dl_list = pd.read_csv(DL_LIST,sep=',|;', engine='python')
dl_list2 = dl_list.applymap(lambda x: x.lower())

#Global Column MANDATORY
df = pd.read_csv(filename, usecols=['EMP_NO','USERNAME','FIRST_NAME','LAST_NAME','EMAIL','AD','EXCHANGE','MANAGER','POSITION','CITY','SUPERVISOR','PASSWORD','WORK_LOCATION','COST_CENTER','MOBILE_PHONE','TICKET','COS'], sep=',|;', engine='python')

#---------- Join table 1 for zimbra------------------
maindf = cos_list.merge(df, how = 'inner', on = ['COS'])
print (maindf)

## Split email address to sAMAccountName and limit just 20 Char in there because of AD limitation
sAMAccountName_field = maindf.EMAIL.str.split("@").str[0].str.slice(0, 20)
maindf['sAMAccountName'] = sAMAccountName_field.str.slice(0, 20)

## Domain name
domain_field = maindf.EMAIL.str.split("@").str[1]
maindf['domain_name'] = domain_field

#Add CN column
cn_field = maindf.FIRST_NAME +" "+ maindf.LAST_NAME
maindf['DisplayName'] = cn_field

#Add date Created column
date_field = datenow
maindf['date_created'] = date_field

#convert to lowercase for spesific column
maindf['COS']=maindf['COS'].str.lower()
maindf['CITY']=maindf['CITY'].str.lower()


#Add new column for zimbra mandatory
maindf['createAccount']='createAccount'
maindf['displayName']='displayName'
maindf['givenName']='givenName'
maindf['sn']='sn'
maindf['pager']='pager'
maindf['telephoneNumber']='telephoneNumber'
maindf['zimbraCOSid']='zimbraCOSid'
maindf['alias']='aaa'
maindf['adlm']='adlm'
#--
maindf['ZEMP_NO']="'"+maindf.EMP_NO.astype('str')+"'"
maindf['alias_domain']=maindf['USERNAME']+'@'+maindf['domain_name']
maindf['ZFIRST_NAME']="'"+maindf['FIRST_NAME']+"'"
maindf['ZLAST_NAME']="'"+maindf['LAST_NAME']+"'"
maindf['Zphone']="'"+maindf.MOBILE_PHONE.astype('str')+"'"

#--

#---------- Join table 2 for zimbra DL------------------
dl_df = dl_list2.merge(maindf, how = 'inner', on = ['COS','CITY'])
print (dl_df)

# DECIDE Field by feature and Position
AD_EXCHANGE = maindf.loc[(maindf.AD.str.lower() == 'y') & (maindf.EXCHANGE.str.lower() == 'y')]
AD_ZIMBRA = maindf.loc[(maindf.AD.str.lower() == 'y') & (maindf.EXCHANGE.str.lower() == 'n')]
zimbraonly = maindf.loc[(maindf.AD.str.lower() == 'n') & (maindf.EXCHANGE.str.lower() == 'n')]



#Global Table AD Format
tbl_adex=['EmployeeNo','FirstName','LastName','DisplayName','UserLogonName','JobTitle','City','Office','Department','EnableMailbox']

# Export to csv Untuk AD format dengan Exchange
CONV_AD_EXCHANGE = AD_EXCHANGE.astype('str').replace('\.0', '', regex=True)
CONV_AD_EXCHANGE.to_csv(ADEX_FILE, sep=',', encoding='utf-8', index = None, header=tbl_adex,columns=['EMP_NO','FIRST_NAME','LAST_NAME','DisplayName','sAMAccountName','POSITION','CITY','WORK_LOCATION','COST_CENTER','EXCHANGE'])

# EXPORT TO CSV FOR untuk  AD_ONLY
CONV_AD_ONLY = AD_ZIMBRA.astype('str').replace('\.0', '', regex=True)
CONV_AD_ONLY.to_csv(ADEX_FILE, mode = 'a' ,sep=',', encoding='utf-8', index = None, header=None,columns=['EMP_NO','FIRST_NAME','LAST_NAME','DisplayName','sAMAccountName','POSITION','CITY','WORK_LOCATION','COST_CENTER','EXCHANGE'])

# EXPORT to csv create zimbra
# Generate file for creating file zimbra
CONV_ZM_AD = AD_ZIMBRA.astype('str').replace('\.0', '', regex=True)
CONV_ZM_ONLY = zimbraonly.astype('str').replace('\.0', '', regex=True)



# FORMAT CREATE ZIMBRA
# create csv file
zmfile = open(ZM_FILE,'w+')
CONV_ZM_AD.to_csv(ZM_FILE,encoding='utf-8',sep=' ', index = None, header=None,mode = 'a', columns=['createAccount','EMAIL','PASSWORD','displayName','DisplayName','givenName','ZFIRST_NAME','sn','ZLAST_NAME','pager','ZEMP_NO','telephoneNumber','Zphone','zimbraCOSid','COS_ID'])
CONV_ZM_ONLY.to_csv(ZM_FILE,encoding='utf-8',sep=' ', index = None, header=None,mode = 'a', columns=['createAccount','EMAIL','PASSWORD','displayName','DisplayName','givenName','ZFIRST_NAME','sn','ZLAST_NAME','pager','ZEMP_NO','telephoneNumber','Zphone','zimbraCOSid','COS_ID'])


# ALIAS IN ZIMBRA
CONV_ZM_AD.to_csv(ZM_FILE,encoding='utf-8',sep=' ', index = None, header=None, mode = 'a', columns=['alias','EMAIL','alias_domain'])
CONV_ZM_ONLY.to_csv(ZM_FILE,encoding='utf-8',sep=' ', index = None, header=None, mode = 'a', columns=['alias','EMAIL','alias_domain'])

# DL IN ZIMBRA
dl_df.to_csv(ZM_FILE,encoding='utf-8',sep=' ', index = None, header=None, mode = 'a', columns=['adlm','dl_address','EMAIL'])
