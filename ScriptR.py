from cassandra.cluster import Cluster
import json
from tabulate import tabulate
import datetime
import csv

ip = input("Cassandra IP (Default 127.0.0.1) : ") or '127.0.0.1'
port = input("Cassandra Port (Default 9242): ") or '9242'
isGetPlatformDetails = input("Retrieve platform details (yes,1,no,0 Default no): ")

cluster = Cluster([ip], port=port, control_connection_timeout=None)
session = cluster.connect('PWC_Keyspace')
session.default_timeout = 30

result = session.execute('SELECT * FROM "CourseAssignment"  WHERE "key" = \'SYSTEM\'')

noOfRecords = 0
noOfAssignmentTemplates = 0
noOfTiiTemplates = 0
noOfTemplWithNoATKey = 0
noOfTemplWithNoTIIKey = 0
tiiTrue = 0
tiiTemplates = []

noOfTiiTemplates_XL = 0
noOfTiiTemplates_CCNG = 0
noOfTiiTemplates_REVEL = 0
noOfTiiTemplates_none = 0
noOfTiiTemplates_other = 0

for iterating_var in result:

	noOfRecords += 1
	jsonObject = json.loads(iterating_var.value)

	try:
		if jsonObject["assignmentTemplate"] == False:
			continue
		else:
			noOfAssignmentTemplates += 1
	except KeyError:
		noOfTemplWithNoATKey += 1
	

	try:
		if jsonObject["tii"] == True :
			tiiTrue += 1
			try:
				creationTime = datetime.datetime.utcfromtimestamp(int(jsonObject["creationDate"])/1000).strftime('%Y-%m-%d %H:%M:%S')
			except ValueError:
				creationTime = "-"
				
			if isGetPlatformDetails == "yes" or isGetPlatformDetails == "1" :
				resultPlatforms = session.execute('SELECT platform FROM "assignmenttemplate_platformdiscipline" WHERE "assignment_template_id" = \'%s\'' % jsonObject["aid"])
				platformArr = []
				listunique = []
				for platforms in resultPlatforms:
					platformArr.append(platforms.platform)
					listunique = list(set(platformArr))
				tiiTemplates.append([jsonObject["aid"], creationTime, str(listunique).replace("[","").replace("'","").replace("u","").replace("]","")])
				#tiiTemplates.append([jsonObject["aid"], creationTime, listunique])
				
				if "REVEL" in platformArr:
					noOfTiiTemplates_REVEL += 1
				elif  "CCNG" in platformArr:
					noOfTiiTemplates_CCNG += 1
				elif  "XL" in platformArr:
					noOfTiiTemplates_XL += 1
				else :
					if len(platformArr) == 0:
						noOfTiiTemplates_none += 1
					else :
						noOfTiiTemplates_other += 1
				
			else :
				tiiTemplates.append([jsonObject["aid"], creationTime, "-"])
			noOfTiiTemplates += 1
	except KeyError:
		noOfTemplWithNoTIIKey += 1
	except IndexError:
		print ('error')

	
headers = ["Assignment ID", "Created Date", "Platform"]
print (tabulate(tiiTemplates, headers, tablefmt="fancy_grid"))

with open("report.csv", "w", newline="") as csvfile:
    a = csv.writer(csvfile, delimiter=',')
    a.writerows(tiiTemplates)
		
		
print ("Total Assignments which have Course as \"SYSTEM\" : %d" % noOfRecords)		
print ("Total Assingment Templates : %d" % noOfAssignmentTemplates)
print ("Total Tii Templates : %d" % noOfTiiTemplates)
print ("Total Tii TRUE Templates : %d" % tiiTrue)
print ("No of Tii Templates mapped with REVEL: %d" % noOfTiiTemplates_REVEL)
print ("No of Tii Templates mapped with XL: %d" % noOfTiiTemplates_XL)
print ("No of Tii Templates mapped with CCNG: %d" % noOfTiiTemplates_CCNG)
print ("No of Tii Templates mapped with Other platforms: %d" % noOfTiiTemplates_other)
print ("No of Tii Templates not mapped with any platform: %d" % noOfTiiTemplates_none)
print ("No of Templates which does not have assignmentTemplate key : %d" % noOfTemplWithNoATKey)
print ("Executed time in UTC : %s" % datetime.datetime.utcnow())




