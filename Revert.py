import csv
from cassandra.cluster import Cluster
import json

ip = input("Cassandra IP (Default 127.0.0.1) : ") or '127.0.0.1'
port = input("Cassandra Port (Default 9242): ") or '9242'

cluster = Cluster([ip], port=port, control_connection_timeout=None)
session = cluster.connect('PWC_Keyspace')
session.default_timeout = 30

with open('report.csv', newline='') as csvfile:
	spamreader = csv.reader(csvfile, delimiter=' ', quotechar='|')
	for row in spamreader:
		x = row[0]
		array = x.split(",")
		print(array[0])
		result = session.execute('SELECT * FROM "CourseAssignment"  WHERE "key" = \'SYSTEM\'')
		for iterating_var in result:
			jsonObject = json.loads(iterating_var.value)
			
			column1 = iterating_var.column1

			if column1 == array[0]:
				jsonObject['tii'] = False
				jsonObjectNew = json.dumps(jsonObject, sort_keys=True)
				session.execute('UPDATE "CourseAssignment" SET value = %s  WHERE key = \'SYSTEM\' AND "column1" = %s', (jsonObjectNew, column1))  
