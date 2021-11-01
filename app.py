from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import os
import urllib

class App():
    
    def __init__(self):
        self.hostname = os.environ.get('hostname', 'mhrijhumber.database.windows.net')
        self.db_server_port = urllib.parse.quote_plus(str(os.environ.get('db_server_port', '8080')))
        self.db_name = os.environ.get('db_name', 'MHIRJ')
        self.db_username = urllib.parse.quote_plus(str(os.environ.get('db_username', 'mhrij')))
        self.db_password = urllib.parse.quote_plus(str(os.environ.get('db_password', 'KaranCool123')))
        self.ssl_mode = urllib.parse.quote_plus(str(os.environ.get('ssl_mode','prefer')))
        self.db_driver = "ODBC Driver 17 for SQL Server"
        self.app = self.initDB()

    def initDB(self):
        app = FastAPI()
        app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
        return app