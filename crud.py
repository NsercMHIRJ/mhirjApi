import pandas as pd
import json
import pyodbc
import os
import urllib
import pandas as pd
import csv
import codecs

# Initialiaze Database Properties
hostname = os.environ.get('hostname', 'mhrijhumber.database.windows.net')
db_server_port = urllib.parse.quote_plus(str(os.environ.get('db_server_port', '8080')))
db_name = os.environ.get('db_name', 'MHIRJ')
db_username = urllib.parse.quote_plus(str(os.environ.get('db_username', 'mhrij')))
db_password = urllib.parse.quote_plus(str(os.environ.get('db_password', 'KaranCool123')))
ssl_mode = urllib.parse.quote_plus(str(os.environ.get('ssl_mode','prefer')))
db_driver = "ODBC Driver 17 for SQL Server"

def insertData(file):
    df = to_df(file)
    df.columns = df.columns.str.replace(' ', '_')
    df.columns = df.columns.str.replace('#', '')
    # Connect to SQL Server
    conn = pyodbc.connect(driver=db_driver, host=hostname, database=db_name,
                              user=db_username, password=db_password)
    cursor = conn.cursor()

    # Create Table
    cursor.execute('''
    CREATE TABLE [dbo].[Airline_MDC_Data_CSV_UPLOAD](
    [Aircraft] [varchar](255) NULL,
    [Tail] [nvarchar](50) NOT NULL,
    [Flight_Leg_No] [int] NULL,
    [ATA_Main] [int] NOT NULL,
    [ATA_Sub] [nvarchar](50) NOT NULL,
    [ATA] [nvarchar](50) NOT NULL,
    [ATA_Description] [nvarchar](100) NOT NULL,
    [LRU] [nvarchar](50) NOT NULL,
    [DateAndTime] [datetime2](7) NOT NULL,
    [MDC_Message] [nvarchar](50) NOT NULL,
    [Status] [nvarchar](50) NOT NULL,
    [Flight_Phase] [nvarchar](50) NULL,
    [Type] [nvarchar](50) NOT NULL,
    [Intermittent] [nvarchar](50) NULL,
    [Equation_ID] [varchar](255) NULL,
    [Source] [nvarchar](50) NOT NULL,
    [Diagnostic_Data] [text] NULL,
    [Data_Used_to_Determine_Msg] [nvarchar](250) NULL,
    [ID] [nvarchar](50) NOT NULL,
    [Flight] [nvarchar](50) NULL,
    [airline_id] [int] NOT NULL,
    [aircraftno] [varchar](255) NULL
    ) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
    ''')

    # Insert DataFrame to Table
    for index,row in df.iterrows():
        print(index)
        cursor.execute('''
INSERT INTO [dbo].[Airline_MDC_Data_CSV_UPLOAD]
           
           ([Aircraft]
           ,[Tail]
           ,[Flight_Leg_No]
           ,[ATA_Main]
           ,[ATA_Sub]
           ,[ATA]
           ,[ATA_Description]
           ,[LRU]
           ,[DateAndTime]
           ,[MDC_Message]
           ,[Status]
           ,[Flight_Phase]
           ,[Type]
           ,[Intermittent]
           ,[Equation_ID]
           ,[Source]
           ,[Diagnostic_Data]
           ,[Data_Used_to_Determine_Msg]
           ,[ID]
           ,[Flight]
           ,[airline_id]
           ,[aircraftno]) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    ''',
                        row.Aircraft,
                        row.Tail,
                        row.Flight_Leg_No,
                        row.ATA_Main,
                        row.ATA_Sub,
                        row.ATA,
                        row.ATA_Description,
                        row.LRU,
                        row.DateAndTime,
                        row.MDC_Message,
                        row.Status,
                        row.Flight_Phase,
                        row.Type,
                        row.Intermittent,
                        row.Equation_ID,
                        row.Source,
                        row.Diagnostic_Data,
                        row.Data_Used_to_Determine_Msg,
                        row.ID,
                        row.Flight,
                        '101',
                        row.Aircraft.replace('AC','')
                    )
        conn.commit()
    return {"message":"Successfully inserted"}
    
def to_df(file):
    data = file.file
    data = csv.reader(codecs.iterdecode(data,'utf-8'), delimiter=',')
    header = data.__next__()
    df = pd.DataFrame(data, columns=header)
    return df