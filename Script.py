from cassandra.cluster import Cluster
import json

ip = input("Cassandra IP (Default 127.0.0.1) : ") or '127.0.0.1'
port = input("Cassandra Port (Default 9242): ") or '9242'

cluster = Cluster([ip], port=port, control_connection_timeout=None)
session = cluster.connect('PWC_Keyspace')
session.default_timeout = 30

result = session.execute('SELECT * FROM "CourseAssignment"  WHERE "key" = \'SYSTEM\'')

for iterating_var in result:

    jsonObject = json.loads(iterating_var.value)

    try:
        try:
            if jsonObject["rowId"] == 'SYSTEM' and jsonObject["tii"] == True:

                    jsonObject['tii'] = False
                    jsonObjectNew = json.dumps(jsonObject, sort_keys=True)
                    
                    column1 = iterating_var.column1
                  
                    session.execute('UPDATE "CourseAssignment" SET value = %s  WHERE key = \'SYSTEM\' AND "column1" = %s', (jsonObjectNew, column1))  
                    print("Column ID " + column1 + " has been updated Sucessfuly.")
            else:
                    column1 = iterating_var.column1
                    print("No TII CourseAssignment found with TII Value TRUE in column ID : "+ column1)
        except IndexError:
            print ("error")
           
    except IndexError:
        print ("error")






