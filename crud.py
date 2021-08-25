import pandas as pd
import json
import pyodbc
import os
import urllib
import pandas as pd
import csv
import codecs
import numpy as np


# Initialiaze Database Properties
hostname = os.environ.get('hostname', 'mhrijhumber.database.windows.net')
db_server_port = urllib.parse.quote_plus(str(os.environ.get('db_server_port', '8080')))
db_name = os.environ.get('db_name', 'MHIRJ')
db_username = urllib.parse.quote_plus(str(os.environ.get('db_username', 'mhrij')))
db_password = urllib.parse.quote_plus(str(os.environ.get('db_password', 'KaranCool123')))
ssl_mode = urllib.parse.quote_plus(str(os.environ.get('ssl_mode','prefer')))
db_driver = "ODBC Driver 17 for SQL Server"

def to_df(file):
    data = file.file
    data = csv.reader(codecs.iterdecode(data,'utf-8'), delimiter=',')
    print("DATA IN to_df:",data)
    header = next(data)
    df = pd.DataFrame(data, columns=header)
    return df

def insertData(file):
    df = to_df(file)
    df.columns = df.columns.str.replace(' ', '_')
    df.columns = df.columns.str.replace('#', '')
    print("DF BEFORE : ")
    print(df)
    
    conn = pyodbc.connect(driver=db_driver, host=hostname, database=db_name,
                              user=db_username, password=db_password)
    cursor = conn.cursor()
    sql = "SELECT Aircraft,Tail,Flight_Leg_No,ATA_Main,ATA_Sub,ATA,ATA_Description,LRU,DateAndTime,MDC_Message,Status,Flight_Phase,Type,Intermittent,Equation_ID,Source,Diagnostic_Data,Data_Used_to_Determine_Msg,ID,Flight FROM [dbo].[Airline_MDC_Data_CSV_UPLOAD]"
    #sql = "SELECT Aircraft, Tail,Flight_Leg_No,ATA_Main,ATA_Sub,ATA,ATA_Description,LRU,DateAndTime,MDC_Message,Status,Flight_Phase,Type,Intermittent,Equation_ID,Source,Diagnostic_Data,Data_Used_to_Determine_Msg,ID,Flight FROM [dbo].[Airline_MDC_Data_CSV_UPLOAD]   WHERE CONVERT(VARCHAR, [DateAndTime], 120) BETWEEN  '2020-12-13 23:56:00%' AND '2020-12-13 23:57:00%' "

    try:
        airline_mdc_all_df = pd.read_sql(sql, conn)
    except pyodbc.Error as err:
        print("Error message:- " + str(err))
    df.drop_duplicates(subset=['Aircraft','Flight_Leg_No','DateAndTime','Flight_Phase','Equation_ID'],inplace=True)

    #current file upload data
    print("DF DATA :")
    print(df.columns)

    #past two weeks df
    print("DF AIRLINE ALL:")
    print(airline_mdc_all_df.columns)
    print("DF DIFF AFTER concat : ")
   # df.reset_index(inplace=True)
   # airline_mdc_all_df.reset_index(inplace=True)
   # df.sort_index(drop=True)
   # airline_mdc_all_df.sort_index(drop=True)
    
    #check = df['Aircraft'].isin(airline_mdc_all_df['Aircraft'])


    #df['check'] = np.where(df['Aircraft'] == airline_mdc_all_df['Aircraft']) 
    #print("check is ",check)
    
    
    #merged_df = airline_mdc_all_df.concat(df, indicator=True, how='outer')
    #changed_rows_df = merged_df[merged_df['_merge'] == 'right_only']
    #changed_rows_df.drop('_merge', axis=1)
    #temp_df = pd.concat([airline_mdc_all_df, df])
    #temp_df = temp_df.reset_index(drop=True)
    #df_gpby = temp_df.groupby(list(temp_df.columns))

    #idx = [x[0] for x in df_gpby.groups.values() if len(x) != 1]
    #temp_df.reindex(idx)
    #print(temp_df)
    #print("After duplicate :")
    #new_df = temp_df.drop_duplicates(subset=['Aircraft','Flight_Leg_No','DateAndTime','Flight_Phase','Equation_ID'],inplace=True,keep=False,indicator=True)
    #airline_mdc_all_df.sort_index(inplace=True)
    #df.reset_index(drop=True) 
    #df.sort_index(inplace=True)
    
    #new_df = airline_mdc_all_df.compare(df)
    #new_df = airline_mdc_all_df[ ~airline_mdc_all_df.isin(df)].dropna()

    #df_diff = airline_mdc_all_df.compare(df)

    #df_diff = pd.concat([df,airline_mdc_all_df])
    #print(new_df)
    #df_diff = df_diff.drop_duplicates(subset=['Aircraft','Flight_Leg_No','DateAndTime','Flight_Phase','Equation_ID'],inplace=True,keep=False)
    #print("After duplicate")
    #print(df_diff)

    #final_df = (airline_mdc_all_df != df).stack()
    #print("FINAL")
    #print(final_df)



    # Create Table
    ##### CREATE TABLE QUERY for Airline_MDC_Data.csv
    # cursor.execute('''
    # IF OBJECT_ID('dbo.Airline_MDC_Data_CSV_UPLOAD', 'U') IS NULL
    
    # CREATE TABLE [dbo].[Airline_MDC_Data_CSV_UPLOAD](
	# [Aircraft] [varchar](255) NOT NULL,
	# [Tail] [nvarchar](50) NOT NULL,
	# [Flight_Leg_No] [int] NOT NULL,
	# [ATA_Main] [int] NOT NULL,
	# [ATA_Sub] [nvarchar](50) NOT NULL,
	# [ATA] [nvarchar](50) NOT NULL,
	# [ATA_Description] [nvarchar](100) NOT NULL,
	# [LRU] [nvarchar](50) NOT NULL,
	# [DateAndTime] [datetime2](7) NOT NULL,
	# [MDC_Message] [nvarchar](50) NOT NULL,
	# [Status] [nvarchar](50) NOT NULL,
	# [Flight_Phase] [nvarchar](50) NOT NULL,
	# [Type] [nvarchar](50) NOT NULL,
	# [Intermittent] [nvarchar](50) NULL,
	# [Equation_ID] [varchar](255) NOT NULL,
	# [Source] [nvarchar](50) NOT NULL,
	# [Diagnostic_Data] [text] NULL,
	# [Data_Used_to_Determine_Msg] [nvarchar](250) NULL,
	# [ID] [nvarchar](50) NOT NULL,
	# [Flight] [nvarchar](50) NULL,
	# [airline_id] [int] NOT NULL,
	# [aircraftno] [varchar](255) NULL,
	# CONSTRAINT airlineMDC_pk PRIMARY KEY (Aircraft,Flight_Leg_No,DateAndTime,Flight_Phase,Equation_ID) 
    # )
    # ''')
    # conn.commit()

    
    # # Insert DataFrame to Table
    # for index,row in df.iterrows():
    #     print(index)
    #     cursor.execute('''
    #         INSERT INTO [dbo].[Airline_MDC_Data_CSV_UPLOAD]
    #        ([Aircraft]
    #        ,[Tail]
    #        ,[Flight_Leg_No]
    #        ,[ATA_Main]
    #        ,[ATA_Sub]
    #        ,[ATA]
    #        ,[ATA_Description]
    #        ,[LRU]
    #        ,[DateAndTime]
    #        ,[MDC_Message]
    #        ,[Status]
    #        ,[Flight_Phase]
    #        ,[Type]
    #        ,[Intermittent]
    #        ,[Equation_ID]
    #        ,[Source]
    #        ,[Diagnostic_Data]
    #        ,[Data_Used_to_Determine_Msg]
    #        ,[ID]
    #        ,[Flight]
    #        ,[airline_id]
    #        ,[aircraftno]) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)           
    #     ''',
    #         row.Aircraft,
    #         row.Tail,
    #         row.Flight_Leg_No,
    #         row.ATA_Main,
    #         row.ATA_Sub,
    #         row.ATA,
    #         row.ATA_Description,
    #         row.LRU,
    #         row.DateAndTime,
    #         row.MDC_Message,
    #         row.Status,
    #         row.Flight_Phase,
    #         row.Type,
    #         row.Intermittent,
    #         row.Equation_ID,
    #         row.Source,
    #         row.Diagnostic_Data,
    #         row.Data_Used_to_Determine_Msg,
    #         row.ID,
    #         row.Flight,
    #         '101',
    #         row.Aircraft.replace('AC','')
    #     )
    # conn.commit()
    return {"message":"Successfully inserted into Airline_MDC_Data_CSV"}


def insertData_MDCMessageInputs(file):
    df = to_df(file)
    print(df.columns)
    print("MDCMESS::\n",df)
    df.columns = df.columns.str.replace(' ', '_')
    df.columns = df.columns.str.replace('#', '')
    df.columns = df.columns.str.replace('-', '_')
    # Connect to SQL Server
    print("MDC INPUT ::: ",df.columns)
    conn = pyodbc.connect(driver=db_driver, host=hostname, database=db_name,
                              user=db_username, password=db_password)
    cursor = conn.cursor()

    ##### CREATE TABLE QUERY for MDCMessageInputs.csv
    cursor.execute('''
    IF OBJECT_ID('dbo.MDCMessagesInputs_CSV_UPLOAD', 'U') IS NULL
    CREATE TABLE [dbo].[MDCMessagesInputs_CSV_UPLOAD](
	[LRU] [varchar](max) NULL,
	[ATA] [varchar](max) NULL,
	[Message] [varchar](max) NULL,
	[Comp_ID] [varchar](max) NULL,
	[Message1] [varchar](max) NULL,
	[Fault_Logged] [varchar](max) NULL,
	[Status] [varchar](max) NULL,
	[Message_Type] [varchar](max) NULL,
	[EICAS] [varchar](4000) NULL,
	[Timer] [nvarchar](max) NULL,
	[Logic] [nvarchar](max) NULL,
	[Equation_Description] [varchar](4000) NULL,
	[Equation_ID] [varchar](255) NULL,
	[Occurance_Flag] [nvarchar](max) NULL,
	[Days_Count] [nvarchar](max) NULL,
	[Priority] [varchar](4000) NULL,
	[MHIRJ_ISE_Recommended_Action] [varchar](4000) NULL,
	[Additional_Comments] [varchar](4000) NULL,
	[MHIRJ_ISE_inputs] [varchar](4000) NULL,
    [MEL_or_No_Dispatch] [varchar](4000) NULL
    )
    ''')
    conn.commit()

    #INSERT DATAFRAME INTO TABLE
    for index,row in df.iterrows():
        print("DATA INPUT : ",row.LRU)
        cursor.execute('''
        INSERT INTO [dbo].[MDCMessagesInputs_CSV_UPLOAD](
        [LRU],
        [ATA],
        [Message], 
        [Comp_ID], 
        [Message1], 
        [Fault_Logged], 
        [Status], 
        [Message_Type],
        [EICAS], 
        [Timer], 
        [Logic], 
        [Equation_Description],
        [Equation_ID], 
        [Occurance_Flag], 
        [Days_Count], 
        [Priority], 
        [MHIRJ_ISE_Recommended_Action],
        [Additional_Comments], 
        [MHIRJ_ISE_inputs],
        [MEL_or_No_Dispatch]
        ) 
        VALUES 
        (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ''',
        row.LRU,
        row.ATA,
        row.Message,
        row.Comp_ID,
        row.Message1,
        row.Fault_Logged,
        row.Status,
        row.Message_Type,
        row.EICAS,
        row.Timer,
        row.Logic,
        row.Equation_Description,
        row.Equation_ID,
        row.Occurance_Flag,
        row.Days_Count,
        row.Priority,
        row.MHIRJ_ISE_Recommended_Action,
        row.Additional_Comments,
        row.MHIRJ_ISE_inputs,
        row.MEL_or_No_Dispatch
        )
        conn.commit()
    return {"message":"Successfully inserted into MDCMessagesInputs"}


def insertData_TopMessageSheet(file):
    df = to_df(file)
    df.columns = df.columns.str.replace(' ', '_')
    df.columns = df.columns.str.replace('#', '')
    df.columns = df.columns.str.replace(' / ', '_')
    df.columns = df.columns.str.replace('-', '_')
    print("\n-------COLUMNS",df.columns)
    # Connect to SQL Server
    conn = pyodbc.connect(driver=db_driver, host=hostname, database=db_name,
                              user=db_username, password=db_password)
    cursor = conn.cursor()
    print("\n\n-----------\nDATA IN TOP\n\n\n: ",df)
    ##### CREATE TABLE QUERY for TopMessageSheet.csv
    cursor.execute('''
      IF OBJECT_ID('dbo.TopMessagesSheet_CSV_UPLOAD', 'U') IS NULL
        CREATE TABLE [dbo].[TopMessagesSheet_CSV_UPLOAD](
	[ATA] [nvarchar](max) NOT NULL,
	[CRJ_700] [nvarchar](max) NOT NULL,
	[CRJ_900] [nvarchar](max) NOT NULL,
	[CRJ_1000] [nvarchar](max) NOT NULL,
	[MDC_B1_Code] [varchar](255) NULL,
	[LRU] [nvarchar](max) NOT NULL,
	[Msg_type] [nvarchar](max) NOT NULL,
	[Phase_logged] [nvarchar](max) NOT NULL,
	[EICAS_FDE] [nvarchar](max) NULL,
	[MDC_Msg] [nvarchar](max) NULL,
	[FIM_Task_Reference] [varchar](4000) NULL,
	[In_Service_BA_document] [varchar](4000) NULL,
	[Title] [nvarchar](max) NULL,
	[Known_nuisance] [varchar](4000) NULL,
	[Remarks] [varchar](4000) NULL
    )
    ''')    
    conn.commit()


    # INSERT DATAFRAME INTO TABLE
    for index,row in df.iterrows():
        print("\n\n--index is : ",row.ATA)
        cursor.execute('''
        INSERT INTO [dbo].[TopMessagesSheet_CSV_UPLOAD](
        ATA,
        CRJ_700,
        CRJ_900,
        CRJ_1000,MDC_B1_Code,LRU,Msg_type,Phase_logged,EICAS_FDE,MDC_Msg,FIM_Task_Reference,
        In_Service_BA_document,Title,Known_nuisance,Remarks) 
        VALUES 
        (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ''',
        row.ATA,row.CRJ_700,row.CRJ_900,row.CRJ_1000,row.MDC_B1_Code,row.LRU,row.Msg_type,row.Phase_logged,
        row.EICAS_FDE,row.MDC_Msg,row.FIM_Task_Reference,
        row.In_Service_BA_document,row.Title,row.Known_nuisance,row.Remarks
        )
        conn.commit()
    return {"message":"Successfully inserted into TopMessageSheet"}

# def connect_database_for_corelation_pid(p_id):
#     sql = """SELECT [mdc_ID], [EQ_ID], [aircraftno], [ATA_Description], [LRU], [DateAndTime], [MDC_Date], 
# 	[MDC_MESSAGE], [EQ_DESCRIPTION], [CAS], [LRU_CODE], [LRU_NAME], [FAULT_LOGGED], [MDC_ATA], 
# 	[mdc_ata_main], [mdc_ata_sub], [Status], [mdc_type]
#     FROM [dbo].[sample_corelation]
#     WHERE p_id = (%s)
#     ORDER BY MDC_Date""" % (p_id)

#     print(sql)

#     try:
#         conn = pyodbc.connect(driver=db_driver, host=hostname,
#                               database=db_name,
#                               user=db_username, password=db_password)
#         corelation_df = pd.read_sql(sql, conn)
#         print('query successful')
#         conn.close()
#         return corelation_df
#     except pyodbc.Error as err:
#         print("Couldn't connect to Server")
#         print("Error message:- " + str(err))