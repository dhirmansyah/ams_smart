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
ZM_FILE='/opt/output/adzimbra.csv'

#Global Column MANDATORY
df = pd.read_csv(filename, usecols=['EMP_NO','USERNAME','FIRST_NAME','LAST_NAME','EMAIL','AD','EXCHANGE','MANAGER','POSITION','CITY','SUPERVISOR','PASSWORD','COST_CENTER','TICKET'], sep=',|;', engine='python')

## Split email address to sAMAccountName and limit just 20 Char in there because of AD limitation
sAMAccountName_field = df.EMAIL.str.split("@").str[0].str.slice(0, 20)
df['sAMAccountName'] = sAMAccountName_field.str.slice(0, 20)

## Domain name
domain_field = df.EMAIL.str.split("@").str[1]
df['domain_name'] = domain_field

#Add CN column
cn_field = df.FIRST_NAME +" "+ df.LAST_NAME
df['cn'] = cn_field

#Add date Created column
date_field = datenow
df['date_created'] = date_field



# DECIDE Field by feature and Position
AD_EXCHANGE = df.loc[(df.AD.str.lower() == 'yes') & (df.EXCHANGE.str.lower() == 'yes')]
AD_ZIMBRA = df.loc[(df.AD.str.lower() == 'yes') & (df.EXCHANGE.str.lower() == 'no')]
zimbraonly = df.loc[(df.AD.str.lower() == 'no') & (df.EXCHANGE.str.lower() == 'no')]



# Printing based on value 1 before export
print ("-----------------------Exchange only--------------------------------")
print(AD_EXCHANGE.astype('str').replace('\.0', '', regex=True))
print
print
print ("-----------------------AD Zimbra--------------------------------")
print(AD_ZIMBRA.astype('str').replace('\.0', '', regex=True))
print
print
print ("-----------------------Zimbra only--------------------------------")
print(zimbraonly.astype('str').replace('\.0', '', regex=True))



#Global Table AD Format
tbl_adex=['employe_number','username','first_name','last_name','email','position','l','supervisior','manager','enable_mailbox','sAMAccountName','cn','department']

# Export to csv ADEX
CONV_AD_EXCHANGE = AD_EXCHANGE.astype('str').replace('\.0', '', regex=True)
CONV_AD_EXCHANGE.to_csv(ADEX_FILE, sep=',', encoding='utf-8', index = None, header=tbl_adex,columns=['EMP_NO','USERNAME','FIRST_NAME','LAST_NAME','EMAIL','POSITION','CITY','SUPERVISOR','MANAGER','EXCHANGE','sAMAccountName','cn','COST_CENTER'])

# EXPORT TO CSV FOR create AD_ONLY
CONV_AD_ONLY = AD_ZIMBRA.astype('str').replace('\.0', '', regex=True)
CONV_AD_ONLY.to_csv(ADEX_FILE, mode = 'a' ,sep=',', encoding='utf-8', index = None, header=None,columns=['EMP_NO','USERNAME','FIRST_NAME','LAST_NAME','EMAIL','POSITION','CITY','SUPERVISOR','MANAGER','EXCHANGE','sAMAccountName','cn','COST_CENTER'])

# EXPORT to csv create zimbra
# Generate file for creating file zimbra
CONV_ZM_AD = AD_ZIMBRA.astype('str').replace('\.0', '', regex=True)

#Add new column for zimbra mandatory
CONV_ZM_AD['createAccount']='createAccount'
CONV_ZM_AD['displayName']='displayName'
CONV_ZM_AD['givenName']='givenName'
CONV_ZM_AD['sn']='sn'
CONV_ZM_AD['pager']='pager'
CONV_ZM_AD['telephoneNumber']='telephoneNumber'
CONV_ZM_AD['phone']='phone'
CONV_ZM_AD['alias']='aaa'
CONV_ZM_AD['alias_domain']=CONV_ZM_AD['USERNAME']+'@'+df['domain_name']
CONV_ZM_AD['FIRST_NAME']="'"+CONV_ZM_AD['FIRST_NAME']+"'"
CONV_ZM_AD['LAST_NAME']="'"+CONV_ZM_AD['LAST_NAME']+"'"
CONV_ZM_AD['EMP_NO']="'"+CONV_ZM_AD['EMP_NO']+"'"
CONV_ZM_AD['phone']="'"+CONV_ZM_AD['phone']+"'"

# FORMAT CREATE ZIMBRA AND ALIAS
CONV_ZM_AD.to_csv(ZM_FILE,encoding='utf-8',sep=' ', index = None, header=None, columns=['createAccount','EMAIL','PASSWORD','displayName','cn','givenName','FIRST_NAME','sn','LAST_NAME','pager','EMP_NO','telephoneNumber','phone'])
# ALIAS IN ZIMBRA
CONV_ZM_AD.to_csv(ZM_FILE,encoding='utf-8',sep=' ', index = None, header=None, mode = 'a', columns=['alias','EMAIL','alias_domain'])


# EXPORT to csv create zimbra only
