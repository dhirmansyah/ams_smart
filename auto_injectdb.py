#!/usr/bin/python
import csv
import pandas as pd
import numpy as np
import time
import sys
import random
import MySQLdb
from pandas import DataFrame
from sqlalchemy import create_engine


# Golbal Variable
datenow=(time.strftime("%Y-%m-%d"))
COS_LIST='/opt/automateams/var/cos_list.csv'
DL_LIST='/opt/automateams/var/dl_list.csv'
filename=sys.argv[1]
ADEX_FILE='/opt/output/adexchange.csv'
ZM_FILE='/opt/output/adzimbra.csv'
IMPORT_DB='/opt/output/importdb.csv'

# Index COS file
cos_list = pd.read_csv(COS_LIST,sep=',|;', engine='python')

# Index DL file
dl_list = pd.read_csv(DL_LIST,sep=',|;', engine='python')
dl_list2 = dl_list.applymap(lambda x: x.lower())

#Global Column MANDATORY
df = pd.read_csv(filename, usecols=['EMP_NO','USERNAME','FIRST_NAME','LAST_NAME','EMAIL','AD','EXCHANGE','MANAGER','POSITION','CITY','SUPERVISOR','PASSWORD','WORK_LOCATION','COST_CENTER','MOBILE_PHONE','TICKET','COS'], sep=',|;', engine='python')

#---------- Join table 1 for zimbra------------------
maindf = cos_list.merge(df, how = 'inner', on = ['COS'])
print('main df print')
print (maindf)

## Split email address to sAMAccountName and limit just 20 Char in there because of AD limitation
sAMAccountName_field = maindf.EMAIL.str.split("@").str[0].str.slice(0, 20)
maindf['sAMAccountName'] = sAMAccountName_field.str.slice(0, 20)
print(sAMAccountName_field)

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
#maindf['status_employee']='status_employee'
#maindf['status_email']='status_email'
#maindf['id_lync_stts_ftime']='id_lync_stts_ftime'
#maindf['id_mobile_stts_ftime']='id_mobile_stts_ftime'
maindf['status_employee']='0'
maindf['status_email']='0'
maindf['id_lync_stts_ftime']='0'
maindf['id_mobile_stts_ftime']='0'
#--
maindf['ZEMP_NO']="'"+maindf.EMP_NO.astype('str')+"'"
maindf['alias_domain']=maindf['USERNAME']+'@'+maindf['domain_name']
maindf['ZFIRST_NAME']="'"+maindf['FIRST_NAME']+"'"
maindf['ZLAST_NAME']="'"+maindf['LAST_NAME']+"'"
maindf['Zphone']="'"+maindf.MOBILE_PHONE.astype('str')+"'"

#--

#---------- Join table 2 for zimbra DL------------------
dl_df = dl_list2.merge(maindf, how = 'inner', on = ['COS','CITY'])
print ('print dl_df')
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

#create_user_imprtdb = maindf[['EMP_NO','USERNAME','sAMAccountName','PASSWORD','FIRST_NAME','LAST_NAME','DisplayName','EMAIL','POSITION','COST_CENTER','WORK_LOCATION','status_employee','status_email','date_created','TICKET','id_lync_stts_ftime','id_mobile_stts_ftime','MOBILE_PHONE']]

tbl_db=['nik','username','upn','password','f_name','l_name','d_name','email','position','department','office','status_employee','status_email','date_created','otrs_date_created','id_lync_stts','id_mobile_stts','mobile_phone']

maindf.to_csv(IMPORT_DB,encoding='utf-8',sep=',',index = None, quotechar='"', header=tbl_db,columns=['EMP_NO','USERNAME','sAMAccountName','PASSWORD','FIRST_NAME','LAST_NAME','DisplayName','EMAIL','POSITION','COST_CENTER','WORK_LOCATION','status_employee','status_email','date_created','TICKET','id_lync_stts_ftime','id_mobile_stts_ftime','MOBILE_PHONE'])


#------------------------------------- INSERT DB AMS
#-- DB AMS
engine = create_engine("mysql://root:paganini@localhost/homecredit")
#Base.metadata.create_all(engine)

insertdb = pd.read_csv(IMPORT_DB)
insertdb.to_sql(con=engine, index=False, name='tbl_users', if_exists='replace')
#insertdb.to_sql(con=engine, index=False, name='tbl_users', if_exists='append')
#insertdb.to_sql()

#ams = MySQLdb.connect(host= "localhost",
#                  user="root",
#                  passwd="paganini",
#                  db="homecredit")

#mycursor = ams.cursor()
#create_employee = "INSERT INTO tbl_users (nik,username,upn,password,f_name,l_name,d_name,email,position,department,office,status_employee,status_email,date_created,otrs_date_created,id_lync_stts,id_mobile_stts,mobile_phone) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

#number_of_rows = mycursor.executemany(create_employee, printtest)

#conn.commit()
#db.close()

#print(mycursor.rowcount, "record inserted.")
