#!/usr/bin/python
import csv
import pandas as pd
import numpy as np
import time
import sys
import random
import scpclient
import md5
import subprocess
import os
import socket
import smtplib
from pandas import DataFrame
from sqlalchemy import create_engine


# Golbal Variable
datenow=(time.strftime("%Y-%m-%d"))
COS_LIST='/opt/automateams/var/cos_list.csv'
CATALOG='/opt/automateams/var/catalog.csv'
filename=sys.argv[1]
ADEX_FILE='/opt/output/adexchange-'+datenow+'.csv'
ZM_FILE='/opt/output/adzimbra-'+datenow+'.csv'
IMPORT_DB='/opt/output/importdb-'+datenow+'.csv'
ZM_SVR="10.58.122.209"
#DB Engine
engine = create_engine("mysql://root:paganini@localhost/homecredit")


#dataframe database_check
query_check = "select nik,email from tbl_users"
source_db = pd.read_sql(query_check, engine)
source_db.columns=['EMP_NO','EMAIL']
print(source_db)


#Global Column MANDATORY
df = pd.read_csv(filename, usecols=['EMP_NO','USERNAME','FIRST_NAME','LAST_NAME','EMAIL','AD','EXCHANGE','MANAGER','POSITION','CITY','SUPERVISOR','PASSWORD','WORK_LOCATION','COST_CENTER','MOBILE_PHONE','TICKET','POS_ID','EMPLOYEE_NAME'], sep=',|;', engine='python')

# Index Catalog Distribution List file
dl_list = pd.read_csv(CATALOG,sep=',|;', engine='python').apply(lambda x: x.astype(str).str.lower())
# Convert Distribution List to lowercase
conv_dl_list = dl_list.applymap(lambda x: x.lower())


#common = pd.merge(dftest, source_db, left_on = 'EMP_NO', right_on = 'nik')
common = df.merge(source_db,on=['EMP_NO'])
print('-----------common-------------------------')
print(common)

df = df[(~df.EMP_NO.isin(common.EMP_NO))]
print(df)


## Split email address to sAMAccountName and limit just 20 Char in there because of AD limitation
# Purpose 1 : split dari almat email dan di batasi 20 Char
#sAMAccountName_field = df.EMAIL.str.split("@").str[0].str.slice(0, 20).str.lower()
#df['sAMAccountName'] = sAMAccountName_field.str.slice(0, 20).str.lower()
#print(sAMAccountName_field)

# ------------- Modif Kolom AD  / remove special char-------------------------------------
# Purpose 2 : gabung dari FNAME(1 char) + LNAME

sAMAccountName_fname = df.FIRST_NAME.str.slice(0, 1).str.lower().str.replace(",","").str.replace("-","").str.replace("\'","")
sAMAccountName_lname = df.LAST_NAME.str.slice(0, 18).str.lower().str.replace(",","").str.replace("-","").str.replace("\'","")
df['sAMAccountName'] = sAMAccountName_fname+'.'+sAMAccountName_lname
#----

df['FIRST_NAME'] = df.FIRST_NAME.str.replace(',','').str.replace("-","").str.replace("\'","")
df['LAST_NAME'] = df.LAST_NAME.str.replace('\,',"").str.replace("\-","").str.replace("\'","")
df['EMAIL'] = df.EMAIL.str.lower().str.replace(",","").str.replace("-","").str.replace("\'","")
df['USERNAME'] = df.USERNAME.str.lower().str.replace(",","").str.replace("-","").str.replace("\'","")
domain_field = df.EMAIL.str.split("@").str[1]
df['domain_name'] = domain_field

#Add date Created column
date_field = datenow
df['date_created'] = date_field

#convert to lowercase for spesific column
df['POS_ID']=df['POS_ID'].str.lower()
df['CITY']=df['CITY'].str.lower()

#Add colom for insert to database by default value
df['status_employee']='0'
df['status_email']='0'
df['id_lync_stts_ftime']='0'
df['id_mobile_stts_ftime']='0'

#Add new column for zimbra mandatory
df['createAccount']='createAccount'
df['displayName']='displayName'
df['givenName']='givenName'
df['sn']='sn'
df['pager']='pager'
df['telephoneNumber']='telephoneNumber'
df['zimbraCOSid']='zimbraCOSid'
df['alias']='aaa'
df['adlm']='adlm'

#--
df['ZEMP_NO']="'"+df.EMP_NO.astype('str')+"'"
df['alias_domain']=df['USERNAME']+'@'+df['domain_name']
df['ZFIRST_NAME']="'"+df['FIRST_NAME']+"'"
df['ZLAST_NAME']="'"+df['LAST_NAME']+"'"
df['Zphone']="'"+df.MOBILE_PHONE.astype('str')+"'"

### Validasi jika account sudah ada maka tidak di tampilkan 



# DECIDE Field by feature and Position
AD_EXCHANGE = df.loc[(df.AD.str.lower() == 'y') & (df.EXCHANGE.str.lower() == 'y')]
AD_ZIMBRA = df.loc[(df.AD.str.lower() == 'y') & (df.EXCHANGE.str.lower() == 'n')]
zimbraonly = df.loc[(df.AD.str.lower() == 'n') & (df.EXCHANGE.str.lower() == 'n')]


# Khusus untuk Create Zimbra Join table antara AD_ZIMBRA dan zimbraonly
zm_create = AD_ZIMBRA.append(zimbraonly)


#---------- Join table 2 for zimbra DL------------------
zm_main = conv_dl_list.merge(zm_create, how = 'inner', on = ['POS_ID','CITY'])
#zm_main.to_csv('/opt/output/alldf.csv')


#Main Table AD Format
tbl_adex=['EmployeeNo','FirstName','LastName','DisplayName','UserLogonName','JobTitle','City','Office','Department','EnableMailbox']

# Export to csv Untuk AD format dengan Exchange
CONV_AD_EXCHANGE = AD_EXCHANGE.astype('str').replace('\.0', '', regex=True)
CONV_AD_EXCHANGE.to_csv(ADEX_FILE, sep=',', encoding='utf-8', index = None, header=tbl_adex,columns=['EMP_NO','FIRST_NAME','LAST_NAME','EMPLOYEE_NAME','sAMAccountName','POSITION','CITY','WORK_LOCATION','COST_CENTER','EXCHANGE'])

# EXPORT TO CSV FOR untuk  AD_ONLY
CONV_AD_ONLY = AD_ZIMBRA.astype('str').replace('\.0', '', regex=True)
CONV_AD_ONLY.to_csv(ADEX_FILE, mode = 'a' ,sep=',', encoding='utf-8', index = None, header=None,columns=['EMP_NO','FIRST_NAME','LAST_NAME','EMPLOYEE_NAME','sAMAccountName','POSITION','CITY','WORK_LOCATION','COST_CENTER','EXCHANGE'])

# EXPORT to csv create zimbra
# Generate file for creating file zimbra
CONV_ZM_AD = AD_ZIMBRA.astype('str').replace('\.0', '', regex=True)
CONV_ZM_ONLY = zimbraonly.astype('str').replace('\.0', '', regex=True)

# EXPORT to csv create zimbra
# Generate file for creating file zimbra
#CONV_ZM = zm_create.astype('str').replace('\.0', '', regex=True)
CONV_ZM = zm_main.astype('str').replace('\.0', '', regex=True)

# FORMAT CREATE ZIMBRA
zmfile = open(ZM_FILE,'w+')
CONV_ZM.to_csv(ZM_FILE,encoding='utf-8',sep=' ', index = None, header=None,mode = 'a', columns=['createAccount','EMAIL','PASSWORD','displayName','EMPLOYEE_NAME','givenName','ZFIRST_NAME','sn','ZLAST_NAME','pager','ZEMP_NO','telephoneNumber','Zphone','zimbraCOSid','COS_ID'])

# ALIAS IN ZIMBRA
CONV_ZM.to_csv(ZM_FILE,encoding='utf-8',sep=' ', index = None, header=None, mode = 'a', columns=['alias','EMAIL','alias_domain'])

#ddress DL IN ZIMBR
zm_main.to_csv(ZM_FILE,encoding='utf-8',sep=' ', index = None, header=None, mode = 'a', columns=['adlm','dl_address','EMAIL'])

# ---------------- DB Segment ------------------------------
tbl_db=['nik','username','upn','password','f_name','l_name','d_name','email','position','department','office','status_employee','status_email','date_created','otrs_date_created','id_lync_stts','id_mobile_stts','mobile_phone']

#add domain @HCG.HOMECREDIT.NET
df['sAMAccountName_db']=df.sAMAccountName+'@HCG.HOMECREDIT.NET'




#------------------------------------- INSERT DB AMS
#-- DB AMS
#df.to_csv(IMPORT_DB,encoding='utf-8',sep=',',index = None, quotechar='"', header=tbl_db,columns=['EMP_NO','USERNAME','sAMAccountName_db','PASSWORD','FIRST_NAME','LAST_NAME','EMPLOYEE_NAME','EMAIL','POSITION','COST_CENTER','WORK_LOCATION','status_employee','status_email','date_created','TICKET','id_lync_stts_ftime','id_mobile_stts_ftime','MOBILE_PHONE'])
#insertdb = pd.read_csv(IMPORT_DB)
#insertdb.to_sql(con=engine, index=False, name='tbl_users', if_exists='replace')


# ------------------------------------- SEND TO SSH and Email for result file
# send file zimbra to server zimbra - SSH
os.system("rsync -avh " +ZM_FILE+ " root@"+ZM_SVR+":/tmp/")
# archive file before remove 
os.system("rsync -avh " +ZM_FILE+ " /opt/output/history/")
# remove zimbra file 
os.system("rm -rf "+ZM_FILE+ ";echo 'file "+ZM_FILE+" success and file aready removed..!'")


# -- send mail 
#os.system("echo 'Hi Team, \n\nWe send you file for creation AD account, please execute in poper systems. \n\nThanks\n\n IT Servers '| mailx -v -r 'amsnew@homecredit.co.id' -s 'AD Creation file "+datenow+"' -a "+ADEX_FILE+" -S smtp=smtp1-int.id.prod doni.hirmansyah01@homecredit.co.id firmandha.noerdiansya@homecredit.co.id")

#---------- validasi koneksi 
#def is_connected(ZM_SVR):
#	try:
#		s = socket.create_connection((ZM_SVR, 22), 2)
#		return True
#	except:
#		pass
#	return False
		#%timeit is_connected(ZM_SVR)

	
