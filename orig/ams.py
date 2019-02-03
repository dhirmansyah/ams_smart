import csv
import sys
import time
import re

datenow=(time.strftime("%Y-%m-%d"))
filename=sys.argv[1]

with open(filename, 'r') as source_data:
	csv_reader = csv.DictReader(source_data)
	
	with open('/root/output/outputfile.csv', 'w', ) as result_file:
		fieldnamestest = ['employe_id','f_name','m_name','l_name','email','username','password', 'l','position','ticketnumber','datenow']
		
		csv_writer = csv.DictWriter(result_file,fieldnames=fieldnamestest,delimiter=",")
		
		#csv_writer.writeheader()
	
		for line in csv_reader:
			#if row['email'] == "*ext.homecredit.co.id":
				csv_writer.writerow(line)
