# python_cassandra
It is not possible to change column data which are in form of objects in Cassendra. This python script will login to specified DB servers and retrive all the data in the given table and if match into given logic it would change the values of the json object. 

First when you run the ScriptR.py it would generate a report (cvs file) stating the inforation in that specific table(Eg: noOfRecords etc).

You have to change the script to match into your table data and needs.

Script.py would alter data in the table JSON. It would iterate through each row in the table and would find 'TII' option and if it is True it would change that value into False.

Revert.py would get the cvs file genterated in first step and revert all the changes
