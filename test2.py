import csv
import sys
import time
import re

datenow=(time.strftime("%Y-%m-%d"))
filename=sys.argv[1]

with open(filename, 'r') as source_data:
	csv_reader = csv.DictReader(source_data)
	
	with open('/root/output/outputfile.csv', 'w', ) as result_file:
		#fieldnamestest = ['EMP_NO','USERNAME','EMPLOYEE_NAME','FIRST_NAME','MIDDLE_NAME','LAST_NAME','POSITION','JOIN_DATE','GENDER','BIRTH_DATE','GRADE','COST_CENTER','STATUS','USERTYPE','USER_STATUS','WORK_LOCATION','CURRENCY_CODE','PAY_FREQUENCY','TAX_TYPE','TAX_STATUS','SALARY','TAXED','SALARY_RECEIVED','BANK','BANK_BRANCH','ACCOUNT_NO','ACCOUNT_NAME','PAYPERIOD','EMPLOYMENT_START_DATE','EMPLOYMENT_END_DATE','TERMINATION_DATE','RESIGN_TYPE','RESIGN_REASON','Email']
		fieldnamestest = ['EMP_NO','USERNAME']	
		csv_writer = csv.DictWriter(result_file,fieldnames=fieldnamestest,delimiter=",")
		
		#csv_writer.writeheader()
	
		for line in csv_reader:
			#if row['email'] == "*ext.homecredit.co.id":
				csv_writer.writerow(line)
