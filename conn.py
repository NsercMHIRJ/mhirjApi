
import pyodbc
import os
import urllib

hostname = os.environ.get('hostname', 'mhrijhumber.database.windows.net')
db_driver = "ODBC Driver 17 for SQL Server"
db_username = urllib.parse.quote_plus(str(os.environ.get('db_username', 'mhrij')))
db_password = urllib.parse.quote_plus(str(os.environ.get('db_password', 'KaranCool123')))
db_name = os.environ.get('db_name', 'MHIRJ')

try :
     
    conn = pyodbc.connect(driver=db_driver, host=hostname, database=db_name,
                              user=db_username, password=db_password)
except pyodbc.Error as err:
        print("Couldn't connect to Server")
        print("Error message:- " + str(err))