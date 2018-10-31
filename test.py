import csv
import pandas as pd
import numpy as np
import time
import sys
import random
from pandas import DataFrame


# Golbal Variable
datenow=(time.strftime("%Y-%m-%d"))
filename=sys.argv[1]
ADEX_FILE='/opt/output/adexchange.csv'
ZM_FILE='/opt/output/adzimbra.csv'
#-- COS Zimbra
zimbraCOSid_DSM='3ee22f55-b213-4f60-afb3-bbba8897ee62'
zimbraCOSid_deskcrs='021b784d-239d-4cd3-8140-930559b9b244'
zimbraCOSid_opsoper='d48562a1-39f7-4443-a4f6-3eaad0588720'
zimbraCOSid_filcol='593729be-e266-47b2-913c-1ca91167821d'
zimbraCOSid_filops='19e325ec-e17c-4dfe-a5a1-6a1e8379c465'
zimbraCOSid_salesoperator='6138e306-4fc8-436e-9d13-be5f1bf3375a'


#Global Column MANDATORY
df = pd.read_csv(filename, usecols=['EMP_NO','USERNAME','FIRST_NAME','LAST_NAME','EMAIL','AD','EXCHANGE','MANAGER','POSITION','CITY','SUPERVISOR','PASSWORD','WORK_LOCATION','COST_CENTER','MOBILE_PHONE','TICKET'], sep=',|;', engine='python')

## Split email address to sAMAccountName and limit just 20 Char in there because of AD limitation
sAMAccountName_field = df.EMAIL.str.split("@").str[0].str.slice(0, 20)
df['sAMAccountName'] = sAMAccountName_field.str.slice(0, 20)

## Domain name
domain_field = df.EMAIL.str.split("@").str[1]
df['domain_name'] = domain_field

#Add CN column
cn_field = df.FIRST_NAME +" "+ df.LAST_NAME
df['DisplayName'] = cn_field

#Add date Created column
date_field = datenow
df['date_created'] = date_field



# DECIDE Field by feature and Position
AD_EXCHANGE = df.loc[(df.AD.str.lower() == 'y') & (df.EXCHANGE.str.lower() == 'y')]
AD_ZIMBRA = df.loc[(df.AD.str.lower() == 'y') & (df.EXCHANGE.str.lower() == 'n')]
zimbraonly = df.loc[(df.AD.str.lower() == 'n') & (df.EXCHANGE.str.lower() == 'n')]



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
#tbl_adex=['employe_number','username','first_name','last_name','email','position','l','supervisior','manager','enable_mailbox','sAMAccountName','cn','department']
tbl_adex=['EmployeeNo','FirstName','LastName','DisplayName','UserLogonName','JobTitle','City','Office','Department','EnableMailbox']

# Export to csv ADEX
CONV_AD_EXCHANGE = AD_EXCHANGE.astype('str').replace('\.0', '', regex=True)
CONV_AD_EXCHANGE.to_csv(ADEX_FILE, sep=',', encoding='utf-8', index = None, header=tbl_adex,columns=['EMP_NO','FIRST_NAME','LAST_NAME','DisplayName','sAMAccountName','POSITION','CITY','WORK_LOCATION','COST_CENTER','EXCHANGE'])

# EXPORT TO CSV FOR create AD_ONLY
CONV_AD_ONLY = AD_ZIMBRA.astype('str').replace('\.0', '', regex=True)
CONV_AD_ONLY.to_csv(ADEX_FILE, mode = 'a' ,sep=',', encoding='utf-8', index = None, header=None,columns=['EMP_NO','FIRST_NAME','LAST_NAME','DisplayName','sAMAccountName','POSITION','CITY','WORK_LOCATION','COST_CENTER','EXCHANGE'])

# EXPORT to csv create zimbra
# Generate file for creating file zimbra
CONV_ZM_AD = AD_ZIMBRA.astype('str').replace('\.0', '', regex=True)
CONV_ZM_ONLY = zimbraonly.astype('str').replace('\.0', '', regex=True)

#Add new column for zimbra mandatory
CONV_ZM_AD['createAccount']='createAccount'
CONV_ZM_AD['displayName']='displayName'
CONV_ZM_AD['givenName']='givenName'
CONV_ZM_AD['sn']='sn'
CONV_ZM_AD['pager']='pager'
CONV_ZM_AD['telephoneNumber']='telephoneNumber'
CONV_ZM_AD['phone']='MOBILE_PHONE'
CONV_ZM_AD['zimbraCOSid']='zimbraCOSid'
CONV_ZM_AD['alias']='aaa'
CONV_ZM_AD['alias_domain']=CONV_ZM_AD['USERNAME']+'@'+df['domain_name']
CONV_ZM_AD['FIRST_NAME']="'"+CONV_ZM_AD['FIRST_NAME']+"'"
CONV_ZM_AD['LAST_NAME']="'"+CONV_ZM_AD['LAST_NAME']+"'"
CONV_ZM_AD['EMP_NO']="'"+CONV_ZM_AD['EMP_NO']+"'"
CONV_ZM_AD['phone']="'"+CONV_ZM_AD['MOBILE_PHONE']+"'"
CONV_ZM_AD['zimbraCOSid_value']=zimbraCOSid_DSM
#--
CONV_ZM_ONLY['createAccount']='createAccount'
CONV_ZM_ONLY['displayName']='displayName'
CONV_ZM_ONLY['givenName']='givenName'
CONV_ZM_ONLY['sn']='sn'
CONV_ZM_ONLY['pager']='pager'
CONV_ZM_ONLY['telephoneNumber']='telephoneNumber'
CONV_ZM_ONLY['phone']='MOBILE_PHONE'
CONV_ZM_ONLY['alias']='aaa'
CONV_ZM_ONLY['alias_domain']=CONV_ZM_ONLY['USERNAME']+'@'+df['domain_name']
CONV_ZM_ONLY['FIRST_NAME']="'"+CONV_ZM_ONLY['FIRST_NAME']+"'"
CONV_ZM_ONLY['LAST_NAME']="'"+CONV_ZM_ONLY['LAST_NAME']+"'"
CONV_ZM_ONLY['EMP_NO']="'"+CONV_ZM_ONLY['EMP_NO']+"'"
CONV_ZM_ONLY['phone']="'"+CONV_ZM_ONLY['MOBILE_PHONE']+"'"


# FORMAT CREATE ZIMBRA AND ALIAS
CONV_ZM_AD.to_csv(ZM_FILE,encoding='utf-8',sep=' ', index = None, header=None, columns=['createAccount','EMAIL','PASSWORD','displayName','cn','givenName','FIRST_NAME','sn','LAST_NAME','pager','EMP_NO','telephoneNumber','phone','zimbraCOSid','zimbraCOSid_value'])
CONV_ZM_ONLY.to_csv(ZM_FILE,encoding='utf-8',sep=' ', index = None, header=None, mode = 'a', columns=['createAccount','EMAIL','PASSWORD','displayName','cn','givenName','FIRST_NAME','sn','LAST_NAME','pager','EMP_NO','telephoneNumber','phone'])

# ALIAS IN ZIMBRA
CONV_ZM_AD.to_csv(ZM_FILE,encoding='utf-8',sep=' ', index = None, header=None, mode = 'a', columns=['alias','EMAIL','alias_domain'])
CONV_ZM_ONLY.to_csv(ZM_FILE,encoding='utf-8',sep=' ', index = None, header=None, mode = 'a', columns=['alias','EMAIL','alias_domain'])


# EXPORT to csv create zimbra only
