import csv
import sys
import time
import re

datenow=(time.strftime("%Y-%m-%d"))
filename=sys.argv[1]

# Generate for Windows system + exchange mail 
#with open(filename, mode='rt') as csv_file:
#    csv_reader = csv.DictReader(csv_file)
#    line_count = 0
#    for row in csv_reader:
#        if line_count == 0:
#            #print(f'{", ".join(row)}')
#            line_count += 1
		
#        print(f'{row["employe_id"]},{row["f_name"]},{row["l_name"]},{row["email"]},{row["username"]},{row["password"]},{row["position"]},{row["l"]},{row["ticketnumber"]},{datenow}')
#        line_count += 1

with open(filename, 'r') as source_data:
	csv_reader = csv.DictReader(source_data)
	
	with open('/root/output/outputfile.csv', 'w', ) as result_file:
		fieldnamestest = ['employe_id','f_name','m_name','l_name','email','username','password', 'l','position','ticketnumber','datenow']
		
		csv_writer = csv.DictWriter(result_file,fieldnames=fieldnamestest,delimiter=",")
		
		#csv_writer.writeheader()
	
		for line in csv_reader:
			#if row['email'] == "*ext.homecredit.co.id":
				csv_writer.writerow(line)
