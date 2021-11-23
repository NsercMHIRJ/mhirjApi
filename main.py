# Importing libraries to the project
#!/usr/bin/bash
from Charts.chart5 import chart5Report
from GenerateReport.history import historyReport
from Charts.chart3 import chart3Report
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
import numbers
import datetime
import time
import sys
#import csv
import pyodbc
import re
import json
#from datetime import datetime
from flask import Flask, jsonify
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.encoders import jsonable_encoder
from typing import Optional
templates = Jinja2Templates(directory="templates")
import os
import urllib
from fastapi import File, UploadFile
from crud import *
from pm_upload import *


app = FastAPI()
"""origins = [

#     "https://GenerateReport/{analysisType}/{occurences}/{legs}/{intermittent}/{consecutiveDays}/{airlineOperator}/{ata}/{messages}/{fromDate}/{toDate}",

#     "http://localhost",
#     "http://localhost:8000",
    
#origins=[
	"http://localhost:*",
    	"http://127.0.0.1:*",
	"http://lively-grass-011bafa10.azurestaticapps.net"
]
# ]
#origins=[*]
Create a list of allowed origins ( as strings)
"""
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

#####

OutputTableHistory2 = pd.DataFrame()
MDCeqns_arrayforgraphing = pd.DataFrame()

# Initialiaze Database Properties
hostname = os.environ.get('hostname', 'mhrijhumber.database.windows.net')
db_server_port = urllib.parse.quote_plus(str(os.environ.get('db_server_port', '8080')))
db_name = os.environ.get('db_name', 'MHIRJ')
db_username = urllib.parse.quote_plus(str(os.environ.get('db_username', 'mhrij')))
db_password = urllib.parse.quote_plus(str(os.environ.get('db_password', 'KaranCool123')))
ssl_mode = urllib.parse.quote_plus(str(os.environ.get('ssl_mode','prefer')))
db_driver = "ODBC Driver 17 for SQL Server"

"""
db_driver = "ODBC Driver 17 for SQL Server"
hostname = "mhirjserver.database.windows.net"
db_name = "MHIRJ"
db_username = "mhirj-admin"
db_password = "KaranCool123"
"""
CurrentFlightPhaseEnabled = 0
MDCdataDF = pd.DataFrame()

def connect_to_fetch_all_jam_messages():
    jam_messages_query = "SELECT * from JamMessagesList"
    try:
        conn = pyodbc.connect(driver=db_driver, host=hostname, database=db_name,
                              user=db_username, password=db_password)
        jam_messages_df = pd.read_sql(jam_messages_query, conn)

        return jam_messages_df
    except pyodbc.Error as err:
        print("Couldn't connect to Server")
        print("Error message:- " + str(err))


listofJamMessages = list()



def convert_array_to_tuple(array_list):
    l1 = []
    l2 = []
    for i in array_list:
        l1.append(i)
        t = tuple(l1)
        l2.append(t)
        l1 = []

    return "(" + ', '.join('''{}'''.format(str(t[0])) for t in l2) + ")"



def connect_to_fetch_all_ata(from_dt, to_dt):
    all_ata_query = "SELECT DISTINCT ATA_Main from Airline_MDC_Data WHERE DateAndTime BETWEEN '" + from_dt + "' AND '" + to_dt + "'"
    try:
        conn = pyodbc.connect(driver=db_driver, host=hostname, database=db_name,
                              user=db_username, password=db_password)
        all_ata_df = pd.read_sql(all_ata_query, conn)

        return all_ata_df
    except pyodbc.Error as err:
        print("Couldn't connect to Server")
        print("Error message:- " + str(err))

def connect_to_fetch_all_eqids(from_dt, to_dt):
    all_ata_query = "SELECT DISTINCT Equation_ID from Airline_MDC_Data WHERE DateAndTime BETWEEN '" + from_dt + "' AND '" + to_dt + "'"
    try:
        conn = pyodbc.connect(driver=db_driver, host=hostname, database=db_name,
                              user=db_username, password=db_password)
        all_eqid_df = pd.read_sql(all_ata_query, conn)

        return all_eqid_df
    except pyodbc.Error as err:
        print("Couldn't connect to Server")
        print("Error message:- " + str(err))

def connect_database_MDCdata(ata, excl_eqid, airline_operator, include_current_message, from_dt, to_dt):
    global MDCdataDF
    global airline_id
    all_ata_str_list = []
    
    if airline_operator.upper() == "SKW":
        airline_id = 101

    if ata == 'ALL':
        all_ata = connect_to_fetch_all_ata(from_dt, to_dt)

        all_ata_str = "("
        all_ata_list = all_ata['ATA_Main'].tolist()
        for each_ata in all_ata_list:
            all_ata_str_list.append(str(each_ata))
            all_ata_str += "'"+str(each_ata)+"'"
            if each_ata != all_ata_list[-1]:
                all_ata_str += ","
            else:
                all_ata_str += ")"
        print(all_ata_str)

    if excl_eqid == 'NONE':
        all_eqid = connect_to_fetch_all_eqids(from_dt, to_dt)

        all_eqid_str = "("
        all_eqid_list = all_eqid['Equation_ID'].tolist()
        for each_eqid in all_eqid_list:
            #all_eqid_str_list.append(str(each_eqid))
            all_eqid_str += "'" + str(each_eqid) + "'"
            if each_eqid != all_eqid_list[-1]:
                all_eqid_str += ","
            else:
                all_eqid_str += ")"
        print(all_eqid_str)

    # If we do not want to include current message -> exclude null flight phase and null intermittents
    if include_current_message == 0:    
        if ata == 'ALL' and excl_eqid == 'NONE':
            sql = "SELECT * FROM Airline_MDC_Data WHERE ATA_Main IN " + str(all_ata_str) + " AND Equation_ID IN " + str(
                all_eqid_str) + " AND airline_id = " + str(
                airline_id) + " AND flight_phase IS NOT NULL AND Intermittent IS NOT NULL AND DateAndTime BETWEEN '" + from_dt + "' AND '" + to_dt + "'"
        elif excl_eqid == 'NONE':
            sql = "SELECT * FROM Airline_MDC_Data WHERE ATA_Main IN " + str(ata) + " AND Equation_ID IN " + str(
                all_eqid_str) + " AND airline_id = " + str(
                airline_id) + " AND flight_phase IS NOT NULL AND Intermittent IS NOT NULL AND DateAndTime BETWEEN '" + from_dt + "' AND '" + to_dt + "'"
        elif ata == 'ALL':
            sql = "SELECT * FROM Airline_MDC_Data WHERE ATA_Main IN " + str(all_ata_str) + " AND Equation_ID NOT IN " + str(
                excl_eqid) + " AND airline_id = " + str(
                airline_id) + " AND flight_phase IS NOT NULL AND Intermittent IS NOT NULL AND DateAndTime BETWEEN '" + from_dt + "' AND '" + to_dt + "'"
        else:
            sql = "SELECT * FROM Airline_MDC_Data WHERE ATA_Main IN " + str(ata) + " AND Equation_ID NOT IN " + str(
                excl_eqid) + " AND airline_id = " + str(
                airline_id) + " AND flight_phase IS NOT NULL AND Intermittent IS NOT NULL AND DateAndTime BETWEEN '" + from_dt + "' AND '" + to_dt + "'"

    elif include_current_message == 1:
        if ata == 'ALL' and excl_eqid =='NONE':
            sql = "SELECT * FROM Airline_MDC_Data WHERE ATA_Main IN " + str(all_ata_str) + " AND Equation_ID IN " + str(
                all_eqid_str) + " AND airline_id = " + str(
                airline_id) + " AND DateAndTime BETWEEN '" + from_dt + "' AND '" + to_dt + "'"
        elif ata == 'ALL':
            sql = "SELECT * FROM Airline_MDC_Data WHERE ATA_Main IN " + str(all_ata_str) + " AND Equation_ID NOT IN " + str(
                excl_eqid) + " AND airline_id = " + str(
                airline_id) + " AND DateAndTime BETWEEN '" + from_dt + "' AND '" + to_dt + "'"
        elif excl_eqid == 'NONE':
            sql = "SELECT * FROM Airline_MDC_Data WHERE ATA_Main IN " + str(ata) + " AND Equation_ID IN " + str(
                all_eqid_str) + " AND airline_id = " + str(
                airline_id) + " AND DateAndTime BETWEEN '" + from_dt + "' AND '" + to_dt + "'"
        else:
            sql = "SELECT * FROM Airline_MDC_Data WHERE ATA_Main IN " + str(ata) + " AND Equation_ID NOT IN " + str(
                excl_eqid) + " AND airline_id = " + str(
                airline_id) + " AND DateAndTime BETWEEN '" + from_dt + "' AND '" + to_dt + "'"

    column_names = ["Aircraft", "Tail#", "Flight Leg No",
               "ATA Main", "ATA Sub", "ATA", "ATA Description", "LRU",
               "DateAndTime", "MDC Message", "Status", "Flight Phase", "Type",
               "Intermittent", "Equation ID", "Source", "Diagnostic Data",
               "Data Used to Determine Msg", "ID", "Flight", "airline_id", "aircraftno"]
    print(sql)
    try:
        conn = pyodbc.connect(driver=db_driver, host=hostname, database=db_name,
                              user=db_username, password=db_password)
        MDCdataDF = pd.read_sql(sql, conn)
        MDCdataDF.columns = column_names
        return MDCdataDF
    except pyodbc.Error as err:
        print("Couldn't connect to Server")
        print("Error message:- " + str(err))

def connect_database_MDCmessagesInputs():
    global MDCMessagesDF
    sql = "SELECT * FROM MDCMessagesInputs"

    try:
        conn = pyodbc.connect(driver=db_driver, host=hostname, database=db_name,
                              user=db_username, password=db_password)
        MDCMessagesDF = pd.read_sql(sql, conn)
        return MDCMessagesDF
    except pyodbc.Error as err:
        print("Couldn't connect to Server")
        print("Error message:- " + err)

def connect_database_TopMessagesSheet():
    global TopMessagesDF
    sql = "SELECT * FROM TopMessagesSheet"

    try:
        conn = pyodbc.connect(driver=db_driver, host=hostname, database=db_name,
                              user=db_username, password=db_password)
        TopMessagesDF = pd.read_sql(sql, conn)
        return TopMessagesDF
    except pyodbc.Error as err:
        print("Couldn't connect to Server")
        print("Error message:- " + err)


@app.post("/api/MDCRawData/{ATAMain_list}/{exclude_EqID_list}/{airline_operator}/{include_current_message}/{fromDate}/{toDate}")
async def get_MDCRawData(ATAMain_list:str, exclude_EqID_list:str, airline_operator:str, include_current_message:int, fromDate: str , toDate: str):
    c = connect_database_MDCdata(ATAMain_list, exclude_EqID_list, airline_operator, include_current_message, fromDate, toDate)
    #print(c['DateAndTime'].astype('datetime64[s]'))
    #c['DateAndTime'] = c['DateAndTime'].astype('datetime64[s]')
    print(c['DateAndTime'])
    print(type(c['DateAndTime']))
    #c['DateAndTime'] = (c['DateAndTime'].to_string()).strip(':00.0000000') #.str.strip(':00.0000000')
    #print(c['DateAndTime'])

    MDCdataDF_json = c.to_json(orient='records')
    return MDCdataDF_json

# @app.post("/api/generateReport/{analysisType}/{occurences}/{legs}/{intermittent}/{consecutiveDays}/{ata}/{exclude_EqID}/{airline_operator}/{include_current_message}/{fromDate}/{toDate}")
# async def generateHistoryReport(analysisType: str, occurences: int, legs: int, intermittent: int, consecutiveDays: int, ata: str, exclude_EqID:str, airline_operator: str, include_current_message: int, fromDate: str , toDate: str):
#     print("Request Data: " + analysisType, occurences, legs, intermittent, consecutiveDays, ata, exclude_EqID, airline_operator, include_current_message, fromDate, toDate)
#     if analysisType.lower() == "history":
#         respObj = historyReport(occurences, legs, intermittent, consecutiveDays, ata, exclude_EqID, airline_operator, include_current_message, fromDate , toDate)

#     return respObj
#for Daily Report: value of consecutiveDays = 0 in URL -> for reference!!       ('32','22')/('B1-007553', 'B1-246748')/skw/1/2020-11-11/2020-11-12
@app.post("/api/GenerateReport/{analysisType}/{occurences}/{legs}/{intermittent}/{consecutiveDays}/{ata}/{exclude_EqID}/{airline_operator}/{include_current_message}/{fromDate}/{toDate}")
async def generateReport(analysisType: str, occurences: int, legs: int, intermittent: int, consecutiveDays: int, ata: str, exclude_EqID:str, airline_operator: str, include_current_message: int, fromDate: str , toDate: str):
    print(fromDate, " ", toDate)
    if analysisType.lower() == "history":
        respObj = historyReport(occurences, legs, intermittent, consecutiveDays, ata, exclude_EqID, airline_operator, include_current_message, fromDate , toDate)
        return respObj
    
    MDCdataDF = connect_database_MDCdata(ata, exclude_EqID, airline_operator, include_current_message, fromDate, toDate)
    print(MDCdataDF)
    # Date formatting
    MDCdataDF["DateAndTime"] = pd.to_datetime(MDCdataDF["DateAndTime"])
    # print(MDCdataDF["DateAndTime"])
    MDCdataDF["Flight Leg No"].fillna(value=0.0, inplace=True)  # Null values preprocessing - if 0 = Currentflightphase
    # print(MDCdataDF["Flight Leg No"])
    MDCdataDF["Flight Phase"].fillna(False, inplace=True)  # NuCell values preprocessing for currentflightphase
    MDCdataDF["Intermittent"].fillna(value=-1, inplace=True)  # Null values preprocessing for currentflightphase
    MDCdataDF["Intermittent"].replace(to_replace=">", value=9,
                                      inplace=True)  # > represents greater than 8 Intermittent values

    try:                      
        #print("data in intermittent ",MDCdataDF["Intermittent"])            
        MDCdataDF["Intermittent"] = int(MDCdataDF["Intermittent"]) # cast type to int
    except:
        #print("data in intermittent exec",MDCdataDF["Intermittent"])            
        MDCdataDF["Intermittent"] = 9


    MDCdataDF["Aircraft"] = MDCdataDF["Aircraft"].str.replace('AC', '')
    MDCdataDF.fillna(value=" ", inplace=True)  # replacing all REMAINING null values to a blank string
    MDCdataDF.sort_values(by= "DateAndTime", ascending= False, inplace= True, ignore_index= True)

    AircraftTailPairDF = MDCdataDF[["Aircraft", "Tail#"]].drop_duplicates(ignore_index= True) # unique pairs of AC SN and Tail# for use in analysis
    AircraftTailPairDF.columns = ["AC SN","Tail#"] # re naming the columns to match History/Daily analysis output

    DatesinData = MDCdataDF["DateAndTime"].dt.date.unique()  # these are the dates in the data in Datetime format.
    NumberofDays = len(MDCdataDF["DateAndTime"].dt.date.unique())  # to pass into Daily analysis number of days in data
    latestDay = str(MDCdataDF.loc[0, "DateAndTime"].date())  # to pass into Daily analysis
    LastLeg = max(MDCdataDF["Flight Leg No"])  # Latest Leg in the data
    MDCdataArray = MDCdataDF.to_numpy()  # converting to numpy to work with arrays

    # MDCMessagesDF = pd.read_csv(MDCMessagesURL_path, encoding="utf8")  # bring messages and inputs into a Dataframe
    MDCMessagesDF = connect_database_MDCmessagesInputs()
    MDCMessagesArray = MDCMessagesDF.to_numpy()  # converting to numpy to work with arrays
    ShapeMDCMessagesArray = MDCMessagesArray.shape  # tuple of the shape of the MDC message data (#rows, #columns)

    # TopMessagesDF = pd.read_csv(TopMessagesURL_path)  # bring messages and inputs into a Dataframe
    TopMessagesDF = connect_database_TopMessagesSheet()
    TopMessagesArray = TopMessagesDF.to_numpy()  # converting to numpy to work with arrays


    UniqueSerialNumArray = []

    if(airline_operator.upper() == "SKW"):
        airline_id = 101
    Flagsreport = 1  # this is here to initialize the variable. user must start report by choosing Newreport = True
    # Flagsreport: int, AircraftSN: str, , newreport: int, CurrentFlightPhaseEnabled: int
    if(include_current_message == 1):
        CurrentFlightPhaseEnabled = 1
    else:
        CurrentFlightPhaseEnabled = 0  # 1 or 0, 1 includes current phase, 0 does not include current phase

    MaxAllowedOccurances = occurences  # flag for Total number of occurences -> T
    MaxAllowedConsecLegs = legs  # flag for consecutive legs -> CF
    MaxAllowedConsecDays = consecutiveDays
    if intermittent > 9:
        MaxAllowedIntermittent = 9  # flag for intermittent values ->IM
    else:
        MaxAllowedIntermittent = intermittent  # flag for intermittent values ->IM
    #Bcode = equationID
    newreport = True    #set a counter variable to bring back it to false

    if(analysisType.lower() == "daily"):

        #global UniqueSerialNumArray

        # function to separate the chunks of data and convert it into a numpy array
        def separate_data(data, date):
            '''Takes data as a dataframe, along with a date to slice the larger data to only include the data in that date'''

            DailyDataDF = data.loc[date]
            return DailyDataDF

        AnalysisDF = MDCdataDF.set_index(
            "DateAndTime")  # since dateandtime was moved to the index of the DF, the column values change from the original MDCdataDF

        currentRow = 0
        MAINtable_array_temp = np.empty((1, 18), object)  # 18 for the date #input from user
        MAINtable_array = []

        # will loop through each day to slice the data for each day, then initialize arrays to individually analyze each day

        for i in range(0, NumberofDays):
            daytopass = str(DatesinData[i])

            # define array to analyze
            DailyanalysisDF = separate_data(AnalysisDF, daytopass)

            ShapeDailyanalysisDF = DailyanalysisDF.shape  # tuple of the shape of the daily data (#rows, #columns)
            DailyanalysisArray = DailyanalysisDF.to_numpy()  # slicing the array to only include the daily data
            NumAC = DailyanalysisDF["Aircraft"].nunique()  # number of unique aircraft SN in the data
            UniqueSerialNumArray = DailyanalysisDF.Aircraft.unique()  # unique aircraft values
            SerialNumFreqSeries = DailyanalysisDF.Aircraft.value_counts()  # the index of this var contains the AC with the most occurrences
            MaxOfAnAC = SerialNumFreqSeries[0]  # the freq series sorts in descending order, max value is top

            # Define the arrays as numpy
            MDCeqns_array = np.empty((MaxOfAnAC, NumAC), object)  # MDC messages for each AC stored in one array
            MDCLegs_array = np.empty((MaxOfAnAC, NumAC),
                                     object)  # Flight Legs for a message for each AC stored in one array
            MDCIntermittent_array = np.empty((MaxOfAnAC, NumAC),
                                             object)  # stores the intermittence values for each message of each array
            FourDigATA_array = np.empty((MaxOfAnAC, NumAC), object)  # stores the ATAs of each message in one array

            if CurrentFlightPhaseEnabled == 1:  # Show all, current and history
                MDCFlightPhase_array = np.ones((MaxOfAnAC, NumAC), int)
            elif CurrentFlightPhaseEnabled == 0:  # Only show history
                MDCFlightPhase_array = np.empty((MaxOfAnAC, NumAC), object)

            Messages_LastRow = ShapeMDCMessagesArray[0]  # taken from the shape of the array (3519 MDC messages total)
            Flags_array = np.empty((Messages_LastRow, NumAC), object)
            FlightLegsEx = 'Flight legs above 32,600 for the following A/C: '  # at 32767 the DCU does not incrementmore the flight counter, so the MDC gets data for the same 32767 over and over until the limit of MDC logs per flight leg is reached (20 msgs per leg), when reached the MDC stops storing data since it gets always the same 32767
            TotalOccurances_array = np.empty((Messages_LastRow, NumAC), int)
            ConsecutiveLegs_array = np.empty((Messages_LastRow, NumAC), int)
            IntermittentInLeg_array = np.empty((Messages_LastRow, NumAC), int)

            # 2D array looping, columns (SNcounter) rows (MDCsheetcounter)
            for SNCounter in range(0, NumAC):  # start counter over each aircraft (columns)

                MDCArrayCounter = 0  # rows of each different array

                for MDCsheetCounter in range(0, ShapeDailyanalysisDF[0]):  # counter over each entry  (rows)

                    # If The Serial number on the dailyanalysisarray matches the current Serial Number, copy
                    if DailyanalysisArray[MDCsheetCounter, 0] == UniqueSerialNumArray[SNCounter]:
                        # Serial numbers match, record information
                        #       SNcounter -->
                        # format for these arrays :   | AC1 | AC2 | AC3 |.... | NumAC
                        # MDCarraycounter(vertically)| xx | xx | xx |...
                        MDCeqns_array[MDCArrayCounter, SNCounter] = DailyanalysisArray[
                            MDCsheetCounter, 13]  # since dateandtime is the index, 13 corresponds to the equations column, where in the history analysis is 14
                        MDCLegs_array[MDCArrayCounter, SNCounter] = DailyanalysisArray[MDCsheetCounter, 2]
                        MDCIntermittent_array[MDCArrayCounter, SNCounter] = DailyanalysisArray[
                            MDCsheetCounter, 12]  # same as above ^
                        FourDigATA_array[MDCArrayCounter, SNCounter] = DailyanalysisArray[MDCsheetCounter, 5]

                        if DailyanalysisArray[MDCsheetCounter, 10]:
                            FourDigATA_array[MDCArrayCounter, SNCounter] = DailyanalysisArray[MDCsheetCounter, 5]

                        if CurrentFlightPhaseEnabled == 0:  # populating the empty array
                            MDCFlightPhase_array[MDCArrayCounter, SNCounter] = DailyanalysisArray[MDCsheetCounter, 10]

                        MDCArrayCounter = MDCArrayCounter + 1

                # arrays with the same size as the MDC messages sheet (3519) checks if each message exists in each ac
                for MessagessheetCounter in range(0, Messages_LastRow):

                    # Initialize Counts, etc

                    # Total Occurances
                    eqnCount = 0

                    # Consecutive Legs
                    ConsecutiveLegs = 0
                    MaxConsecutiveLegs = 0
                    tempLeg = LastLeg

                    # Intermittent
                    IntermittentFlightLegs = 0

                    MDCArrayCounter = 0

                    while MDCArrayCounter < MaxOfAnAC:
                        if MDCeqns_array[MDCArrayCounter, SNCounter]:
                            # Not Empty, and not current                                      B code
                            if MDCeqns_array[MDCArrayCounter, SNCounter] == MDCMessagesArray[MessagessheetCounter, 12] \
                                    and MDCFlightPhase_array[MDCArrayCounter, SNCounter]:

                                # Total Occurances
                                # Count this as 1 occurance
                                eqnCount = eqnCount + 1

                                # Consecutive Days not used in the daily analysis DO FOR HISTORY

                                # Consecutive Legs
                                if MDCLegs_array[MDCArrayCounter, SNCounter] == tempLeg:

                                    tempLeg = tempLeg - 1
                                    ConsecutiveLegs = ConsecutiveLegs + 1

                                    if ConsecutiveLegs > MaxConsecutiveLegs:
                                        MaxConsecutiveLegs = ConsecutiveLegs

                                else:

                                    # If not consecutive, start over
                                    ConsecutiveLegs = 1
                                    tempLeg = MDCLegs_array[MDCArrayCounter, SNCounter]

                                # Intermittent
                                # Taking the maximum intermittent value - come back to this and implement max function for an column
                                x = MDCIntermittent_array[MDCArrayCounter, SNCounter]
                                if isinstance(x, numbers.Number) and MDCIntermittent_array[
                                    MDCArrayCounter, SNCounter] > IntermittentFlightLegs:
                                    IntermittentFlightLegs = MDCIntermittent_array[MDCArrayCounter, SNCounter]
                                # End if Intermittent numeric check

                                # Other
                                # Check that the legs is not over the given limit
                                Flags_array[MessagessheetCounter, SNCounter] = ''
                                if MDCLegs_array[MDCArrayCounter, SNCounter] > 32600:
                                    FlightLegsEx = FlightLegsEx + str(UniqueSerialNumArray[SNCounter]) + ' (' + str(
                                        MDCLegs_array[MDCArrayCounter, SNCounter]) + ')' + ' '
                                # End if Legs flag

                                # Check for Other Flags
                                if MDCMessagesArray[MessagessheetCounter, 13]:
                                    # Immediate (occurrance flag in excel MDC Messages sheet)
                                    if MDCMessagesArray[MessagessheetCounter, 13] == 1:
                                        # Immediate Flag required
                                        Flags_array[MessagessheetCounter, SNCounter] = str(
                                            MDCMessagesArray[MessagessheetCounter, 12]) + " occured at least once."
                            MDCArrayCounter += 1

                        else:
                            MDCArrayCounter = MaxOfAnAC

                            # Next MDCArray Counter

                    TotalOccurances_array[MessagessheetCounter, SNCounter] = eqnCount
                    ConsecutiveLegs_array[MessagessheetCounter, SNCounter] = MaxConsecutiveLegs
                    IntermittentInLeg_array[MessagessheetCounter, SNCounter] = IntermittentFlightLegs

                # Next MessagessheetCounter
            # Next SNCounter

            for SNCounter in range(0, NumAC):

                for EqnCounter in range(0, Messages_LastRow):

                    # Continue with Report
                    if TotalOccurances_array[EqnCounter, SNCounter] >= MaxAllowedOccurances \
                            or ConsecutiveLegs_array[EqnCounter, SNCounter] >= MaxAllowedConsecLegs \
                            or IntermittentInLeg_array[EqnCounter, SNCounter] >= MaxAllowedIntermittent \
                            or Flags_array[EqnCounter, SNCounter]:

                        # Populate Flags Array
                        if TotalOccurances_array[EqnCounter, SNCounter] >= MaxAllowedOccurances:
                            Flags_array[EqnCounter, SNCounter] = Flags_array[
                                                                     EqnCounter, SNCounter] + "Total occurances exceeded " + str(
                                MaxAllowedOccurances) + " occurances. "

                        if ConsecutiveLegs_array[EqnCounter, SNCounter] >= MaxAllowedConsecLegs:
                            Flags_array[EqnCounter, SNCounter] = Flags_array[
                                                                     EqnCounter, SNCounter] + "Maximum consecutive flight legs exceeded " + str(
                                MaxAllowedConsecLegs) + " flight legs. "

                        if IntermittentInLeg_array[EqnCounter, SNCounter] >= MaxAllowedIntermittent:
                            Flags_array[EqnCounter, SNCounter] = Flags_array[
                                                                     EqnCounter, SNCounter] + "Maximum intermittent occurances for one flight leg exceeded " + str(
                                MaxAllowedIntermittent) + " occurances. "

                        # populating the final array (Table)
                        MAINtable_array_temp[0, 0] = daytopass
                        MAINtable_array_temp[0, 1] = UniqueSerialNumArray[SNCounter]
                        MAINtable_array_temp[0, 2] = MDCMessagesArray[EqnCounter, 8]
                        MAINtable_array_temp[0, 3] = MDCMessagesArray[EqnCounter, 4]
                        MAINtable_array_temp[0, 4] = MDCMessagesArray[EqnCounter, 0]
                        MAINtable_array_temp[0, 5] = MDCMessagesArray[EqnCounter, 1]
                        MAINtable_array_temp[0, 6] = MDCMessagesArray[EqnCounter, 12]
                        MAINtable_array_temp[0, 7] = MDCMessagesArray[EqnCounter, 7]
                        MAINtable_array_temp[0, 8] = MDCMessagesArray[EqnCounter, 11]
                        MAINtable_array_temp[0, 9] = TotalOccurances_array[EqnCounter, SNCounter]

                        MAINtable_array_temp[0, 10] = ConsecutiveLegs_array[EqnCounter, SNCounter]
                        MAINtable_array_temp[0, 11] = IntermittentInLeg_array[EqnCounter, SNCounter]
                        MAINtable_array_temp[0, 12] = Flags_array[EqnCounter, SNCounter]

                        # if the input is empty set the priority to 4
                        if MDCMessagesArray[EqnCounter, 15] == 0:
                            MAINtable_array_temp[0, 13] = 4
                        else:
                            MAINtable_array_temp[0, 13] = MDCMessagesArray[EqnCounter, 15]

                        # For B1-006424 & B1-006430 Could MDC Trend tool assign Priority 3 if logged on A/C below 10340, 15317. Priority 1 if logged on 10340, 15317, 19001 and up
                        if MDCMessagesArray[EqnCounter, 12] == "B1-006424" or MDCMessagesArray[
                            EqnCounter, 12] == "B1-006430":
                            if int(UniqueSerialNumArray[SNCounter]) <= 10340 and int(
                                    UniqueSerialNumArray[SNCounter]) > 10000:
                                MAINtable_array_temp[0, 13] = 3
                            elif int(UniqueSerialNumArray[SNCounter]) > 10340 and int(
                                    UniqueSerialNumArray[SNCounter]) < 11000:
                                MAINtable_array_temp[0, 13] = 1
                            elif int(UniqueSerialNumArray[SNCounter]) <= 15317 and int(
                                    UniqueSerialNumArray[SNCounter]) > 15000:
                                MAINtable_array_temp[0, 13] = 3
                            elif int(UniqueSerialNumArray[SNCounter]) > 15317 and int(
                                    UniqueSerialNumArray[SNCounter]) < 16000:
                                MAINtable_array_temp[0, 13] = 1
                            elif int(UniqueSerialNumArray[SNCounter]) >= 19001 and int(
                                    UniqueSerialNumArray[SNCounter]) < 20000:
                                MAINtable_array_temp[0, 13] = 1

                                # check the content of MHIRJ ISE recommendation and add to array
                        if MDCMessagesArray[EqnCounter, 16] == 0:
                            MAINtable_array_temp[0, 15] = ""
                        else:
                            MAINtable_array_temp[0, 15] = MDCMessagesArray[EqnCounter, 16]

                        # check content of "additional"
                        if MDCMessagesArray[EqnCounter, 17] == 0:
                            MAINtable_array_temp[0, 16] = ""
                        else:
                            MAINtable_array_temp[0, 16] = MDCMessagesArray[EqnCounter, 17]

                        # check content of "MHIRJ Input"
                        if MDCMessagesArray[EqnCounter, 18] == 0:
                            MAINtable_array_temp[0, 17] = ""
                        else:
                            MAINtable_array_temp[0, 17] = MDCMessagesArray[EqnCounter, 18]

                        # Check for the equation in the Top Messages sheet
                        TopCounter = 0
                        Top_LastRow = TopMessagesArray.shape[0]
                        while TopCounter < Top_LastRow:

                            # Look for the flagged equation in the Top Messages Sheet
                            if MDCMessagesArray[EqnCounter][12] == TopMessagesArray[TopCounter, 4]:

                                # Found the equation in the Top Messages Sheet. Put the information in the last column
                                MAINtable_array_temp[0, 14] = "Known Nuissance: " + str(TopMessagesArray[TopCounter, 13]) \
                                                              + " / In-Service Document: " + str(
                                    TopMessagesArray[TopCounter, 11]) \
                                                              + " / FIM Task: " + str(TopMessagesArray[TopCounter, 10]) \
                                                              + " / Remarks: " + str(TopMessagesArray[TopCounter, 14])

                                # Not need to keep looking
                                TopCounter = TopMessagesArray.shape[0]

                            else:
                                # Not equal, go to next equation
                                MAINtable_array_temp[0, 14] = ""
                                TopCounter += 1
                        # End while

                        if currentRow == 0:
                            MAINtable_array = np.array(MAINtable_array_temp)
                        else:
                            MAINtable_array = np.append(MAINtable_array, MAINtable_array_temp, axis=0)
                        # End if Build MAINtable_array

                        # Move to next Row on Main page for next flag
                        currentRow = currentRow + 1

        TitlesArrayDaily = ["Date", "AC SN", "EICAS Message", "MDC Message", "LRU", "ATA", "B1-Equation", "Type",
                            "Equation Description", "Total Occurences", "Consecutive FL",
                            "Intermittent", "Reason(s) for flag", "Priority", "Known Top Message - Recommended Documents",
                            "MHIRJ ISE Recommendation", "Additional Comments", "MHIRJ ISE Input"]
        # Converts the Numpy Array to Dataframe to manipulate
        # pd.set_option('display.max_rows', None)
        OutputTableDaily = pd.DataFrame(data=MAINtable_array, columns=TitlesArrayDaily).fillna(" ").sort_values(
            by=["Date", "Type", "Priority"])
        OutputTableDaily = OutputTableDaily.merge(AircraftTailPairDF, on= "AC SN") # Tail # added
        OutputTableDaily = OutputTableDaily[["Tail#", "Date", "AC SN", "EICAS Message", "MDC Message", "LRU", "ATA", "B1-Equation", "Type",
               "Equation Description", "Total Occurences", "Consecutive FL",
              "Intermittent", "Reason(s) for flag", "Priority", "Known Top Message - Recommended Documents",
              "MHIRJ ISE Recommendation", "Additional Comments", "MHIRJ ISE Input"]] # Tail# added to output table which means that column order has to be re ordered
        
        # OutputTableDaily_json = OutputTableDaily.to_json(orient = 'records')
        # OutputTableDaily.to_csv("OutputTableDaily.csv")
        # return OutputTableDaily_json
	
        listofJamMessages = list()
        all_jam_messages = connect_to_fetch_all_jam_messages()
        for each_jam_message in all_jam_messages['Jam_Message']:
            listofJamMessages.append(each_jam_message)
        print(listofJamMessages)

        # highlight function starts here
        OutputTableDaily = OutputTableDaily.assign(
            is_jam=lambda dataframe: dataframe['B1-Equation'].map(lambda c: True if c in listofJamMessages else False)
        )
        print(OutputTableDaily)

        OutputTableDaily_json = OutputTableDaily.to_json(orient='records')
        return OutputTableDaily_json

    # elif(analysisType.lower() == "history"):
    #     #global UniqueSerialNumArray

    #     HistoryanalysisDF = MDCdataDF
    #     ShapeHistoryanalysisDF = HistoryanalysisDF.shape  # tuple of the shape of the history data (#rows, #columns)
    #     HistoryanalysisArray = MDCdataArray
    #     NumAC = HistoryanalysisDF["Aircraft"].nunique()  # number of unique aircraft SN in the data
    #     UniqueSerialNumArray = HistoryanalysisDF.Aircraft.unique()  # unique aircraft values
    #     SerialNumFreqSeries = HistoryanalysisDF.Aircraft.value_counts()  # the index of this var contains the AC with the most occurrences
    #     MaxOfAnAC = SerialNumFreqSeries[0]  # the freq series sorts in descending order, max value is top

    #     # Define the arrays as numpy
    #     MDCeqns_array = np.empty((MaxOfAnAC, NumAC), object)  # MDC messages for each AC stored in one array
    #     MDCDates_array = np.empty((MaxOfAnAC, NumAC), object)  # Dates for a message for each AC stored in one array
    #     MDCLegs_array = np.empty((MaxOfAnAC, NumAC),
    #                              object)  # Flight Legs for a message for each AC stored in one array
    #     MDCIntermittent_array = np.empty((MaxOfAnAC, NumAC),
    #                                      object)  # stores the intermittence values for each message of each array
    #     FourDigATA_array = np.empty((MaxOfAnAC, NumAC), object)  # stores the 4digATAs of each message in one array
    #     TwoDigATA_array = np.empty((MaxOfAnAC, NumAC), object)  # stores the 2digATAs of each message in one array
    #     global MDCeqns_arrayforgraphing
    #     MDCeqns_arrayforgraphing = np.empty((MaxOfAnAC, NumAC),
    #                                         object)  # MDC messages for each AC stored in an array for graphing, due to current messages issue

    #     if CurrentFlightPhaseEnabled == 1:  # Show all, current and history
    #         MDCFlightPhase_array = np.ones((MaxOfAnAC, NumAC), int)
    #     elif CurrentFlightPhaseEnabled == 0:  # Only show history
    #         MDCFlightPhase_array = np.empty((MaxOfAnAC, NumAC), object)

    #     Messages_LastRow = ShapeMDCMessagesArray[0]  # taken from the shape of the array
    #     Flags_array = np.empty((Messages_LastRow, NumAC), object)
    #     FlightLegsEx = 'Flight legs above 32,600 for the following A/C: '  # at 32767 the DCU does not incrementmore the flight counter, so the MDC gets data for the same 32767 over and over until the limit of MDC logs per flight leg is reached (20 msgs per leg), when reached the MDC stops storing data since it gets always the same 32767
    #     TotalOccurances_array = np.empty((Messages_LastRow, NumAC), int)
    #     ConsecutiveDays_array = np.empty((Messages_LastRow, NumAC), int)
    #     ConsecutiveLegs_array = np.empty((Messages_LastRow, NumAC), int)
    #     IntermittentInLeg_array = np.empty((Messages_LastRow, NumAC), int)

    #     # 2D array looping, columns (SNcounter) rows (MDCsheetcounter)
    #     for SNCounter in range(0, NumAC):  # start counter over each aircraft (columns)

    #         MDCArrayCounter = 0  # rows of each different array

    #         for MDCsheetCounter in range(0, ShapeHistoryanalysisDF[0]):  # counter over each entry  (rows)

    #             # If The Serial number on the historyanalysisarray matches the current Serial Number, copy
    #             if HistoryanalysisArray[MDCsheetCounter, 0] == UniqueSerialNumArray[SNCounter]:
    #                 # Serial numbers match, record information
    #                 #       SNcounter -->
    #                 # format for these arrays :   | AC1 | AC2 | AC3 |.... | NumAC
    #                 # MDCarraycounter(vertically)| xx | xx | xx |...
    #                 MDCeqns_array[MDCArrayCounter, SNCounter] = HistoryanalysisArray[MDCsheetCounter, 14]
    #                 MDCDates_array[MDCArrayCounter, SNCounter] = HistoryanalysisArray[MDCsheetCounter, 8]
    #                 MDCLegs_array[MDCArrayCounter, SNCounter] = HistoryanalysisArray[MDCsheetCounter, 2]
    #                 MDCIntermittent_array[MDCArrayCounter, SNCounter] = HistoryanalysisArray[MDCsheetCounter, 13]

    #                 if HistoryanalysisArray[MDCsheetCounter, 11]:  # populating counts array
    #                     FourDigATA_array[MDCArrayCounter, SNCounter] = HistoryanalysisArray[MDCsheetCounter, 5]
    #                     TwoDigATA_array[MDCArrayCounter, SNCounter] = HistoryanalysisArray[MDCsheetCounter, 3]

    #                 if CurrentFlightPhaseEnabled == 0:  # populating the empty array
    #                     MDCFlightPhase_array[MDCArrayCounter, SNCounter] = HistoryanalysisArray[MDCsheetCounter, 11]

    #                 MDCArrayCounter = MDCArrayCounter + 1

    #         # arrays with the same size as the MDC messages sheet (3519) checks if each message exists in each ac
    #         for MessagessheetCounter in range(0, Messages_LastRow):

    #             # Initialize Counts, etc

    #             # Total Occurances
    #             eqnCount = 0

    #             # Consecutive Days
    #             ConsecutiveDays = 0
    #             MaxConsecutiveDays = 0
    #             tempDate = pd.to_datetime(latestDay)
    #             DaysCount = 0

    #             # Consecutive Legs
    #             ConsecutiveLegs = 0
    #             MaxConsecutiveLegs = 0
    #             tempLeg = LastLeg

    #             # Intermittent
    #             IntermittentFlightLegs = 0

    #             MDCArrayCounter = 0

    #             while MDCArrayCounter < MaxOfAnAC:
    #                 if MDCeqns_array[MDCArrayCounter, SNCounter]:
    #                     # Not Empty, and not current                                      B code
    #                     if MDCeqns_array[MDCArrayCounter, SNCounter] == MDCMessagesArray[MessagessheetCounter, 12] \
    #                             and MDCFlightPhase_array[MDCArrayCounter, SNCounter]:

    #                         MDCeqns_arrayforgraphing[MDCArrayCounter, SNCounter] = MDCeqns_array[
    #                             MDCArrayCounter, SNCounter]

    #                         # Total Occurances
    #                         # Count this as 1 occurance
    #                         eqnCount = eqnCount + 1

    #                         # Consecutive Days
    #                         currentdate = pd.to_datetime(MDCDates_array[MDCArrayCounter, SNCounter])
    #                         # first date and fivedaysafter
    #                         # if a flag was raised in the previous count, it has to be reset and a new fivedaysafter is declared
    #                         if eqnCount == 1 or flag == True:
    #                             flag = False
    #                             FiveDaysAfter = currentdate + datetime.timedelta(5)

    #                         # by checking when its even or odd, we can check if the message occurred twice in 5 days
    #                         # if the second time it occurred is below fivedaysafter, flag is true
    #                         # if the second time it occurred is greater than fivedaysafter, flag is false
    #                         # if eqncount is odd, flag is false and new fivedaysafter is declared
    #                         if (eqnCount % 2) == 0:
    #                             if currentdate <= FiveDaysAfter:
    #                                 flag = True
    #                             else:
    #                                 flag = False
    #                         else:
    #                             FiveDaysAfter = currentdate + datetime.timedelta(5)
    #                             flag = False

    #                         if currentdate.day == tempDate.day \
    #                                 and currentdate.month == tempDate.month \
    #                                 and currentdate.year == tempDate.year:

    #                             DaysCount = 1  # 1 because consecutive means 1 day since it occured
    #                             tempDate = tempDate - datetime.timedelta(1)
    #                             ConsecutiveDays = ConsecutiveDays + 1

    #                             if ConsecutiveDays >= MaxConsecutiveDays:
    #                                 MaxConsecutiveDays = ConsecutiveDays

    #                         elif MDCDates_array[MDCArrayCounter, SNCounter] < tempDate:

    #                             # If not consecutive, start over
    #                             if ConsecutiveDays >= MaxConsecutiveDays:
    #                                 MaxConsecutiveDays = ConsecutiveDays

    #                             ConsecutiveDays = 1
    #                             # Days count is the delta between this current date and the previous temp date
    #                             DaysCount += abs(tempDate - currentdate).days + 1
    #                             tempDate = currentdate - datetime.timedelta(1)

    #                         # Consecutive Legs
    #                         if MDCLegs_array[MDCArrayCounter, SNCounter] == tempLeg:

    #                             tempLeg = tempLeg - 1
    #                             ConsecutiveLegs = ConsecutiveLegs + 1

    #                             if ConsecutiveLegs > MaxConsecutiveLegs:
    #                                 MaxConsecutiveLegs = ConsecutiveLegs

    #                         else:

    #                             # If not consecutive, start over
    #                             ConsecutiveLegs = 1
    #                             tempLeg = MDCLegs_array[MDCArrayCounter, SNCounter]

    #                         # Intermittent
    #                         # Taking the maximum intermittent value
    #                         x = MDCIntermittent_array[MDCArrayCounter, SNCounter]
    #                         #if isinstance(x, numbers.Number) and MDCIntermittent_array[MDCArrayCounter, SNCounter] > IntermittentFlightLegs:
    #                         if MDCIntermittent_array[MDCArrayCounter, SNCounter] > IntermittentFlightLegs:
    #                             IntermittentFlightLegs = MDCIntermittent_array[MDCArrayCounter, SNCounter]
    #                         # End if Intermittent numeric check

    #                         # Other
    #                         # Check that the legs is not over the given limit
    #                         Flags_array[MessagessheetCounter, SNCounter] = ''
    #                         if MDCLegs_array[MDCArrayCounter, SNCounter] > 32600:
    #                             FlightLegsEx = FlightLegsEx + str(UniqueSerialNumArray[SNCounter]) + ' (' + str(MDCLegs_array[MDCArrayCounter, SNCounter]) + ')' + ' '
    #                         # End if Legs flag

    #                         # Check for Other Flags
    #                         if MDCMessagesArray[MessagessheetCounter, 13]:
    #                             #Immediate (occurrance flag in excel MDC Messages inputs sheet) - JAM RELATED FLAGS
    #                             if MDCMessagesArray[MessagessheetCounter, 13] == 1 and MDCMessagesArray[MessagessheetCounter,14] == 0:
    #                                 # Immediate Flag required
    #                                 # lIST OF MESSAGES TO BE FLAGGED AS SOON AS POSTED
    #                                 # ["B1-309178","B1-309179","B1-309180","B1-060044","B1-060045","B1-007973",
    #                                 # "B1-060017","B1-006551","B1-240885","B1-006552","B1-006553","B1-006554",
    #                                 # "B1-006555","B1-007798","B1-007772","B1-240938","B1-007925","B1-007905",
    #                                 # "B1-007927","B1-007915","B1-007926","B1-007910","B1-007928","B1-007920"]
    #                                 Flags_array[MessagessheetCounter, SNCounter] = str(
    #                                     MDCMessagesArray[MessagessheetCounter, 12]) + " occured at least once."


    #                             elif MDCMessagesArray[MessagessheetCounter, 13] == 2 and \
    #                                     MDCMessagesArray[MessagessheetCounter, 14] == 5 and \
    #                                     flag == True:
    #                                 # Triggered twice in 5 days
    #                                 # "B1-008350","B1-008351","B1-008360","B1-008361"
    #                                 Flags_array[MessagessheetCounter, SNCounter] = str(MDCMessagesArray[
    #                                                                                        MessagessheetCounter, 12]) + " occured at least twice in 5 days. "
    #                     MDCArrayCounter += 1

    #                 else:
    #                     MDCArrayCounter = MaxOfAnAC

    #                     # Next MDCArray Counter

    #             TotalOccurances_array[MessagessheetCounter, SNCounter] = eqnCount
    #             ConsecutiveDays_array[MessagessheetCounter, SNCounter] = MaxConsecutiveDays
    #             ConsecutiveLegs_array[MessagessheetCounter, SNCounter] = MaxConsecutiveLegs
    #             IntermittentInLeg_array[MessagessheetCounter, SNCounter] = IntermittentFlightLegs
    #         # Next MessagessheetCounter
    #     # Next SNCounter

    #     MAINtable_array_temp = np.empty((1, 18), object)  # 18 because its history #????????
    #     currentRow = 0
    #     MAINtable_array = []
    #     for SNCounter in range(0, NumAC):
    #         for EqnCounter in range(0, Messages_LastRow):

    #             # Continue with Report
    #             if TotalOccurances_array[EqnCounter, SNCounter] >= MaxAllowedOccurances \
    #             or ConsecutiveDays_array[EqnCounter, SNCounter] >= MaxAllowedConsecDays \
    #             or ConsecutiveLegs_array[EqnCounter, SNCounter] >= MaxAllowedConsecLegs \
    #             or IntermittentInLeg_array[EqnCounter, SNCounter] >= MaxAllowedIntermittent \
    #             or Flags_array[EqnCounter, SNCounter]:

    #                 # Populate Flags Array
    #                 if TotalOccurances_array[EqnCounter, SNCounter] >= MaxAllowedOccurances:
    #                     Flags_array[EqnCounter, SNCounter] = Flags_array[EqnCounter, SNCounter] + "Total occurances exceeded " + str(MaxAllowedOccurances) + " occurances. "

    #                 if ConsecutiveDays_array[EqnCounter, SNCounter] >= MaxAllowedConsecDays:
    #                     Flags_array[EqnCounter, SNCounter] = Flags_array[EqnCounter, SNCounter] + "Maximum consecutive days exceeded " + str(MaxAllowedConsecDays) + " days. "

    #                 if ConsecutiveLegs_array[EqnCounter, SNCounter] >= MaxAllowedConsecLegs:
    #                     Flags_array[EqnCounter, SNCounter] = Flags_array[EqnCounter, SNCounter] + "Maximum consecutive flight legs exceeded " + str(MaxAllowedConsecLegs) + " flight legs. "

    #                 if IntermittentInLeg_array[EqnCounter, SNCounter] >= MaxAllowedIntermittent:
    #                     Flags_array[EqnCounter, SNCounter] = Flags_array[EqnCounter, SNCounter] + "Maximum intermittent occurances for one flight leg exceeded " + str(MaxAllowedIntermittent) + " occurances. "

    #                 # populating the final array (Table)
    #                 MAINtable_array_temp[0, 0] = UniqueSerialNumArray[SNCounter]
    #                 MAINtable_array_temp[0, 1] = MDCMessagesArray[EqnCounter, 8]
    #                 MAINtable_array_temp[0, 2] = MDCMessagesArray[EqnCounter, 4]
    #                 MAINtable_array_temp[0, 3] = MDCMessagesArray[EqnCounter, 0]
    #                 MAINtable_array_temp[0, 4] = MDCMessagesArray[EqnCounter, 1]
    #                 MAINtable_array_temp[0, 5] = MDCMessagesArray[EqnCounter, 12]
    #                 MAINtable_array_temp[0, 6] = MDCMessagesArray[EqnCounter, 7]
    #                 MAINtable_array_temp[0, 7] = MDCMessagesArray[EqnCounter, 11]
    #                 MAINtable_array_temp[0, 8] = TotalOccurances_array[EqnCounter, SNCounter]
    #                 MAINtable_array_temp[0, 9] = ConsecutiveDays_array[EqnCounter, SNCounter]
    #                 MAINtable_array_temp[0, 10] = ConsecutiveLegs_array[EqnCounter, SNCounter]
    #                 MAINtable_array_temp[0, 11] = IntermittentInLeg_array[EqnCounter, SNCounter]
    #                 MAINtable_array_temp[0, 12] = Flags_array[EqnCounter, SNCounter]

    #                 # if the input is empty set the priority to 4
    #                 if MDCMessagesArray[EqnCounter, 15] == 0:
    #                     MAINtable_array_temp[0, 13] = 4
    #                 else:
    #                     MAINtable_array_temp[0, 13] = MDCMessagesArray[EqnCounter, 15]

    #                 # For B1-006424 & B1-006430 Could MDC Trend tool assign Priority 3 if logged on A/C below 10340, 15317. Priority 1 if logged on 10340, 15317, 19001 and up
    #                 if MDCMessagesArray[EqnCounter, 12] == "B1-006424" or MDCMessagesArray[
    #                     EqnCounter, 12] == "B1-006430":
    #                     if int(UniqueSerialNumArray[SNCounter]) <= 10340 and int(
    #                             UniqueSerialNumArray[SNCounter]) > 10000:
    #                         MAINtable_array_temp[0, 13] = 3
    #                     elif int(UniqueSerialNumArray[SNCounter]) > 10340 and int(
    #                             UniqueSerialNumArray[SNCounter]) < 11000:
    #                         MAINtable_array_temp[0, 13] = 1
    #                     elif int(UniqueSerialNumArray[SNCounter]) <= 15317 and int(
    #                             UniqueSerialNumArray[SNCounter]) > 15000:
    #                         MAINtable_array_temp[0, 13] = 3
    #                     elif int(UniqueSerialNumArray[SNCounter]) > 15317 and int(
    #                             UniqueSerialNumArray[SNCounter]) < 16000:
    #                         MAINtable_array_temp[0, 13] = 1
    #                     elif int(UniqueSerialNumArray[SNCounter]) >= 19001 and int(
    #                             UniqueSerialNumArray[SNCounter]) < 20000:
    #                         MAINtable_array_temp[0, 13] = 1

    #                 # check the content of MHIRJ ISE recommendation and add to array
    #                 if MDCMessagesArray[EqnCounter, 16] == 0:
    #                     MAINtable_array_temp[0, 15] = ""
    #                 else:
    #                     MAINtable_array_temp[0, 15] = MDCMessagesArray[EqnCounter, 16]

    #                 # check content of "additional"
    #                 if MDCMessagesArray[EqnCounter, 17] == 0:
    #                     MAINtable_array_temp[0, 16] = ""
    #                 else:
    #                     MAINtable_array_temp[0, 16] = MDCMessagesArray[EqnCounter, 17]

    #                 # check content of "MHIRJ Input"
    #                 if MDCMessagesArray[EqnCounter, 18] == 0:
    #                     MAINtable_array_temp[0, 17] = ""
    #                 else:
    #                     MAINtable_array_temp[0, 17] = MDCMessagesArray[EqnCounter, 18]

    #                 # Check for the equation in the Top Messages sheet
    #                 TopCounter = 0
    #                 Top_LastRow = TopMessagesArray.shape[0]
    #                 while TopCounter < Top_LastRow:

    #                     # Look for the flagged equation in the Top Messages Sheet
    #                     if MDCMessagesArray[EqnCounter][12] == TopMessagesArray[TopCounter, 4]:

    #                         # Found the equation in the Top Messages Sheet. Put the information in the last column
    #                         MAINtable_array_temp[0, 14] = "Known Nuissance: " + str(TopMessagesArray[TopCounter, 13]) \
    #                                                       + " / In-Service Document: " + str(TopMessagesArray[TopCounter, 11]) \
    #                                                       + " / FIM Task: " + str(TopMessagesArray[TopCounter, 10]) \
    #                                                       + " / Remarks: " + str(TopMessagesArray[TopCounter, 14])

    #                         # Not need to keep looking
    #                         TopCounter = TopMessagesArray.shape[0]

    #                     else:
    #                         # Not equal, go to next equation
    #                         MAINtable_array_temp[0, 14] = ""
    #                         TopCounter += 1
    #                 # End while

    #                 if currentRow == 0:
    #                     MAINtable_array = np.array(MAINtable_array_temp)
    #                 else:
    #                     MAINtable_array = np.append(MAINtable_array, MAINtable_array_temp, axis=0)
    #                 # End if Build MAINtable_array

    #                 # Move to next Row on Main page for next flag
    #                 currentRow = currentRow + 1
    #     TitlesArrayHistory = ["AC SN", "EICAS Message", "MDC Message", "LRU", "ATA", "B1-Equation", "Type",
    #                           "Equation Description", "Total Occurences", "Consective Days", "Consecutive FL",
    #                           "Intermittent", "Reason(s) for flag", "Priority",
    #                           "Known Top Message - Recommended Documents",
    #                           "MHIRJ ISE Recommendation", "Additional Comments", "MHIRJ ISE Input"]

    #     # Converts the Numpy Array to Dataframe to manipulate
    #     # pd.set_option('display.max_rows', None)
    #     # Main table
    #     global OutputTableHistory
    #     OutputTableHistory = pd.DataFrame(data=MAINtable_array, columns=TitlesArrayHistory).fillna(" ").sort_values(
    #         by=["Type", "Priority"])
    #     OutputTableHistory = OutputTableHistory.merge(AircraftTailPairDF, on="AC SN")  # Tail # added
    #     OutputTableHistory = OutputTableHistory[
    #         ["Tail#", "AC SN", "EICAS Message", "MDC Message", "LRU", "ATA", "B1-Equation", "Type",
    #          "Equation Description", "Total Occurences", "Consective Days", "Consecutive FL",
    #          "Intermittent", "Reason(s) for flag", "Priority", "Known Top Message - Recommended Documents",
    #          "MHIRJ ISE Recommendation", "Additional Comments",
    #          "MHIRJ ISE Input"]]  # Tail# added to output table which means that column order has to be re ordered
    #     #OutputTableHistory.to_csv("OutputTableHistory.csv")
    #     #OutputTableHistory_json = OutputTableHistory.to_json(orient = 'records')
    #     #return OutputTableHistory_json
	
	# # Get the list of JAM Messages.
    #     listofJamMessages = list()
    #     all_jam_messages = connect_to_fetch_all_jam_messages()
    #     for each_jam_message in all_jam_messages['Jam_Message']:
    #         listofJamMessages.append(each_jam_message)
    #     print(listofJamMessages)

    #     # HIGHLIGHT function starts here
    #     OutputTableHistory = OutputTableHistory.assign(
    #         is_jam=lambda dataframe: dataframe['B1-Equation'].map(lambda c: True if c in listofJamMessages else False)
    #     )
    #     print(OutputTableHistory)

    #     OutputTableHistory_json = OutputTableHistory.to_json(orient='records')
    #     return OutputTableHistory_json


# Jams Flag Report
# function used later on to display the messages in the same Flight Leg as the Jam flags raised
def flagsinreport(OutputTable, Aircraft, listofmessages=listofJamMessages):
    ''' display the messages in the same Flight Leg as the Jam flags
        OutputTable: Either HistoryReport or Dailyreport
        Listofmessages: Jam Messages
        Return: Dataframe indexed with AC SN and Flight Leg #
    '''
    datatofilter = MDCdataDF.copy(deep=True)
    # print(datatofilter)
    isin = OutputTable["B1-Equation"].isin(listofmessages)
    filter1 = OutputTable[isin][["AC SN", "B1-Equation"]]
    listoftuplesACID = list(zip(filter1["AC SN"], filter1["B1-Equation"]))

    datatofilter2 = datatofilter.set_index(["Aircraft", "Equation ID"]).sort_index().loc[
                    pd.IndexSlice[listoftuplesACID], :].reset_index()
    listoftuplesACFL = list(zip(datatofilter2["Aircraft"], datatofilter2["Flight Leg No"]))

    datatofilter3 = datatofilter.set_index(["Aircraft", "Flight Leg No"]).sort_index()
    FinalDF = datatofilter3.loc[pd.IndexSlice[listoftuplesACFL], :]
    print(FinalDF)
    return FinalDF.loc[Aircraft]

@app.post(
    "/api/GenerateReport/{analysisType}/{occurences}/{legs}/{intermittent}/{consecutiveDays}/{ata}/{exclude_EqID}/{airline_operator}/{include_current_message}/{fromDate}/{toDate}/{ACSN_chosen}")
async def generateJamsReport(analysisType: str, occurences: int, legs: int, intermittent: int, consecutiveDays: int,
                         ata: str, exclude_EqID: str, airline_operator: str, include_current_message: int,
                         fromDate: str, toDate: str, ACSN_chosen:int):
    print(fromDate, " ", toDate)

    MDCdataDF = connect_database_MDCdata(ata, exclude_EqID, airline_operator, include_current_message, fromDate, toDate)
    print(MDCdataDF)
    # Date formatting
    MDCdataDF["DateAndTime"] = pd.to_datetime(MDCdataDF["DateAndTime"])
    # print(MDCdataDF["DateAndTime"])
    MDCdataDF["Flight Leg No"].fillna(value=0.0, inplace=True)  # Null values preprocessing - if 0 = Currentflightphase
    # print(MDCdataDF["Flight Leg No"])
    MDCdataDF["Flight Phase"].fillna(False, inplace=True)  # NuCell values preprocessing for currentflightphase
    MDCdataDF["Intermittent"].fillna(value=-1, inplace=True)  # Null values preprocessing for currentflightphase
    MDCdataDF["Intermittent"].replace(to_replace=">", value="9",
                                      inplace=True)  # > represents greater than 8 Intermittent values
    MDCdataDF["Intermittent"] = MDCdataDF["Intermittent"].astype(int)  # cast type to int

    MDCdataDF["Aircraft"] = MDCdataDF["Aircraft"].str.replace('AC', '')
    MDCdataDF.fillna(value=" ", inplace=True)  # replacing all REMAINING null values to a blank string
    MDCdataDF.sort_values(by="DateAndTime", ascending=False, inplace=True, ignore_index=True)

    AircraftTailPairDF = MDCdataDF[["Aircraft", "Tail#"]].drop_duplicates(
        ignore_index=True)  # unique pairs of AC SN and Tail# for use in analysis
    AircraftTailPairDF.columns = ["AC SN", "Tail#"]  # re naming the columns to match History/Daily analysis output

    DatesinData = MDCdataDF["DateAndTime"].dt.date.unique()  # these are the dates in the data in Datetime format.
    NumberofDays = len(MDCdataDF["DateAndTime"].dt.date.unique())  # to pass into Daily analysis number of days in data
    latestDay = str(MDCdataDF.loc[0, "DateAndTime"].date())  # to pass into Daily analysis
    LastLeg = max(MDCdataDF["Flight Leg No"])  # Latest Leg in the data
    MDCdataArray = MDCdataDF.to_numpy()  # converting to numpy to work with arrays

    # MDCMessagesDF = pd.read_csv(MDCMessagesURL_path, encoding="utf8")  # bring messages and inputs into a Dataframe
    MDCMessagesDF = connect_database_MDCmessagesInputs()
    MDCMessagesArray = MDCMessagesDF.to_numpy()  # converting to numpy to work with arrays
    ShapeMDCMessagesArray = MDCMessagesArray.shape  # tuple of the shape of the MDC message data (#rows, #columns)

    # TopMessagesDF = pd.read_csv(TopMessagesURL_path)  # bring messages and inputs into a Dataframe
    TopMessagesDF = connect_database_TopMessagesSheet()
    TopMessagesArray = TopMessagesDF.to_numpy()  # converting to numpy to work with arrays

    UniqueSerialNumArray = []

    if (airline_operator.upper() == "SKW"):
        airline_id = 101
    Flagsreport = 1  # this is here to initialize the variable. user must start report by choosing Newreport = True
    # Flagsreport: int, AircraftSN: str, , newreport: int, CurrentFlightPhaseEnabled: int
    if (include_current_message == 1):
        CurrentFlightPhaseEnabled = 1
    else:
        CurrentFlightPhaseEnabled = 0  # 1 or 0, 1 includes current phase, 0 does not include current phase

    MaxAllowedOccurances = occurences  # flag for Total number of occurences -> T
    MaxAllowedConsecLegs = legs  # flag for consecutive legs -> CF
    if intermittent > 9:
        MaxAllowedIntermittent = 9  # flag for intermittent values ->IM
    else:
        MaxAllowedIntermittent = intermittent  # flag for intermittent values ->IM
    MaxAllowedConsecDays = consecutiveDays
    # Bcode = equationID
    newreport = True  # set a counter variable to bring back it to false

    if (analysisType == "daily"):

        # global UniqueSerialNumArray

        # function to separate the chunks of data and convert it into a numpy array
        def separate_data(data, date):
            '''Takes data as a dataframe, along with a date to slice the larger data to only include the data in that date'''

            DailyDataDF = data.loc[date]
            return DailyDataDF

        AnalysisDF = MDCdataDF.set_index(
            "DateAndTime")  # since dateandtime was moved to the index of the DF, the column values change from the original MDCdataDF

        currentRow = 0
        MAINtable_array_temp = np.empty((1, 18), object)  # 18 for the date #input from user
        MAINtable_array = []

        # will loop through each day to slice the data for each day, then initialize arrays to individually analyze each day

        for i in range(0, NumberofDays):
            daytopass = str(DatesinData[i])

            # define array to analyze
            DailyanalysisDF = separate_data(AnalysisDF, daytopass)

            ShapeDailyanalysisDF = DailyanalysisDF.shape  # tuple of the shape of the daily data (#rows, #columns)
            DailyanalysisArray = DailyanalysisDF.to_numpy()  # slicing the array to only include the daily data
            NumAC = DailyanalysisDF["Aircraft"].nunique()  # number of unique aircraft SN in the data
            UniqueSerialNumArray = DailyanalysisDF.Aircraft.unique()  # unique aircraft values
            SerialNumFreqSeries = DailyanalysisDF.Aircraft.value_counts()  # the index of this var contains the AC with the most occurrences
            MaxOfAnAC = SerialNumFreqSeries[0]  # the freq series sorts in descending order, max value is top

            # Define the arrays as numpy
            MDCeqns_array = np.empty((MaxOfAnAC, NumAC), object)  # MDC messages for each AC stored in one array
            MDCLegs_array = np.empty((MaxOfAnAC, NumAC),
                                     object)  # Flight Legs for a message for each AC stored in one array
            MDCIntermittent_array = np.empty((MaxOfAnAC, NumAC),
                                             object)  # stores the intermittence values for each message of each array
            FourDigATA_array = np.empty((MaxOfAnAC, NumAC), object)  # stores the ATAs of each message in one array

            if CurrentFlightPhaseEnabled == 1:  # Show all, current and history
                MDCFlightPhase_array = np.ones((MaxOfAnAC, NumAC), int)
            elif CurrentFlightPhaseEnabled == 0:  # Only show history
                MDCFlightPhase_array = np.empty((MaxOfAnAC, NumAC), object)

            Messages_LastRow = ShapeMDCMessagesArray[0]  # taken from the shape of the array (3519 MDC messages total)
            Flags_array = np.empty((Messages_LastRow, NumAC), object)
            FlightLegsEx = 'Flight legs above 32,600 for the following A/C: '  # at 32767 the DCU does not incrementmore the flight counter, so the MDC gets data for the same 32767 over and over until the limit of MDC logs per flight leg is reached (20 msgs per leg), when reached the MDC stops storing data since it gets always the same 32767
            TotalOccurances_array = np.empty((Messages_LastRow, NumAC), int)
            ConsecutiveLegs_array = np.empty((Messages_LastRow, NumAC), int)
            IntermittentInLeg_array = np.empty((Messages_LastRow, NumAC), int)

            # 2D array looping, columns (SNcounter) rows (MDCsheetcounter)
            for SNCounter in range(0, NumAC):  # start counter over each aircraft (columns)

                MDCArrayCounter = 0  # rows of each different array

                for MDCsheetCounter in range(0, ShapeDailyanalysisDF[0]):  # counter over each entry  (rows)

                    # If The Serial number on the dailyanalysisarray matches the current Serial Number, copy
                    if DailyanalysisArray[MDCsheetCounter, 0] == UniqueSerialNumArray[SNCounter]:
                        # Serial numbers match, record information
                        #       SNcounter -->
                        # format for these arrays :   | AC1 | AC2 | AC3 |.... | NumAC
                        # MDCarraycounter(vertically)| xx | xx | xx |...
                        MDCeqns_array[MDCArrayCounter, SNCounter] = DailyanalysisArray[
                            MDCsheetCounter, 13]  # since dateandtime is the index, 13 corresponds to the equations column, where in the history analysis is 14
                        MDCLegs_array[MDCArrayCounter, SNCounter] = DailyanalysisArray[MDCsheetCounter, 2]
                        MDCIntermittent_array[MDCArrayCounter, SNCounter] = DailyanalysisArray[
                            MDCsheetCounter, 12]  # same as above ^
                        FourDigATA_array[MDCArrayCounter, SNCounter] = DailyanalysisArray[MDCsheetCounter, 5]

                        if DailyanalysisArray[MDCsheetCounter, 10]:
                            FourDigATA_array[MDCArrayCounter, SNCounter] = DailyanalysisArray[MDCsheetCounter, 5]

                        if CurrentFlightPhaseEnabled == 0:  # populating the empty array
                            MDCFlightPhase_array[MDCArrayCounter, SNCounter] = DailyanalysisArray[MDCsheetCounter, 10]

                        MDCArrayCounter = MDCArrayCounter + 1

                # arrays with the same size as the MDC messages sheet (3519) checks if each message exists in each ac
                for MessagessheetCounter in range(0, Messages_LastRow):

                    # Initialize Counts, etc

                    # Total Occurances
                    eqnCount = 0

                    # Consecutive Legs
                    ConsecutiveLegs = 0
                    MaxConsecutiveLegs = 0
                    tempLeg = LastLeg

                    # Intermittent
                    IntermittentFlightLegs = 0

                    MDCArrayCounter = 0

                    while MDCArrayCounter < MaxOfAnAC:
                        if MDCeqns_array[MDCArrayCounter, SNCounter]:
                            # Not Empty, and not current                                      B code
                            if MDCeqns_array[MDCArrayCounter, SNCounter] == MDCMessagesArray[MessagessheetCounter, 12] \
                                    and MDCFlightPhase_array[MDCArrayCounter, SNCounter]:

                                # Total Occurances
                                # Count this as 1 occurance
                                eqnCount = eqnCount + 1

                                # Consecutive Days not used in the daily analysis DO FOR HISTORY

                                # Consecutive Legs
                                if MDCLegs_array[MDCArrayCounter, SNCounter] == tempLeg:

                                    tempLeg = tempLeg - 1
                                    ConsecutiveLegs = ConsecutiveLegs + 1

                                    if ConsecutiveLegs > MaxConsecutiveLegs:
                                        MaxConsecutiveLegs = ConsecutiveLegs

                                else:

                                    # If not consecutive, start over
                                    ConsecutiveLegs = 1
                                    tempLeg = MDCLegs_array[MDCArrayCounter, SNCounter]

                                # Intermittent
                                # Taking the maximum intermittent value - come back to this and implement max function for an column
                                x = MDCIntermittent_array[MDCArrayCounter, SNCounter]
                                if isinstance(x, numbers.Number) and MDCIntermittent_array[
                                    MDCArrayCounter, SNCounter] > IntermittentFlightLegs:
                                    IntermittentFlightLegs = MDCIntermittent_array[MDCArrayCounter, SNCounter]
                                # End if Intermittent numeric check

                                # Other
                                # Check that the legs is not over the given limit
                                Flags_array[MessagessheetCounter, SNCounter] = ''
                                if MDCLegs_array[MDCArrayCounter, SNCounter] > 32600:
                                    FlightLegsEx = FlightLegsEx + str(UniqueSerialNumArray[SNCounter]) + ' (' + str(
                                        MDCLegs_array[MDCArrayCounter, SNCounter]) + ')' + ' '
                                # End if Legs flag

                                # Check for Other Flags
                                if MDCMessagesArray[MessagessheetCounter, 13]:
                                    # Immediate (occurrance flag in excel MDC Messages sheet)
                                    if MDCMessagesArray[MessagessheetCounter, 13] == 1:
                                        # Immediate Flag required
                                        Flags_array[MessagessheetCounter, SNCounter] = str(
                                            MDCMessagesArray[MessagessheetCounter, 12]) + " occured at least once."
                            MDCArrayCounter += 1

                        else:
                            MDCArrayCounter = MaxOfAnAC

                            # Next MDCArray Counter

                    TotalOccurances_array[MessagessheetCounter, SNCounter] = eqnCount
                    ConsecutiveLegs_array[MessagessheetCounter, SNCounter] = MaxConsecutiveLegs
                    IntermittentInLeg_array[MessagessheetCounter, SNCounter] = IntermittentFlightLegs

                # Next MessagessheetCounter
            # Next SNCounter

            for SNCounter in range(0, NumAC):

                for EqnCounter in range(0, Messages_LastRow):

                    # Continue with Report
                    if TotalOccurances_array[EqnCounter, SNCounter] >= MaxAllowedOccurances \
                            or ConsecutiveLegs_array[EqnCounter, SNCounter] >= MaxAllowedConsecLegs \
                            or IntermittentInLeg_array[EqnCounter, SNCounter] >= MaxAllowedIntermittent \
                            or Flags_array[EqnCounter, SNCounter]:

                        # Populate Flags Array
                        if TotalOccurances_array[EqnCounter, SNCounter] >= MaxAllowedOccurances:
                            Flags_array[EqnCounter, SNCounter] = Flags_array[
                                                                     EqnCounter, SNCounter] + "Total occurances exceeded " + str(
                                MaxAllowedOccurances) + " occurances. "

                        if ConsecutiveLegs_array[EqnCounter, SNCounter] >= MaxAllowedConsecLegs:
                            Flags_array[EqnCounter, SNCounter] = Flags_array[
                                                                     EqnCounter, SNCounter] + "Maximum consecutive flight legs exceeded " + str(
                                MaxAllowedConsecLegs) + " flight legs. "

                        if IntermittentInLeg_array[EqnCounter, SNCounter] >= MaxAllowedIntermittent:
                            Flags_array[EqnCounter, SNCounter] = Flags_array[
                                                                     EqnCounter, SNCounter] + "Maximum intermittent occurances for one flight leg exceeded " + str(
                                MaxAllowedIntermittent) + " occurances. "

                        # populating the final array (Table)
                        MAINtable_array_temp[0, 0] = daytopass
                        MAINtable_array_temp[0, 1] = UniqueSerialNumArray[SNCounter]
                        MAINtable_array_temp[0, 2] = MDCMessagesArray[EqnCounter, 8]
                        MAINtable_array_temp[0, 3] = MDCMessagesArray[EqnCounter, 4]
                        MAINtable_array_temp[0, 4] = MDCMessagesArray[EqnCounter, 0]
                        MAINtable_array_temp[0, 5] = MDCMessagesArray[EqnCounter, 1]
                        MAINtable_array_temp[0, 6] = MDCMessagesArray[EqnCounter, 12]
                        MAINtable_array_temp[0, 7] = MDCMessagesArray[EqnCounter, 7]
                        MAINtable_array_temp[0, 8] = MDCMessagesArray[EqnCounter, 11]
                        MAINtable_array_temp[0, 9] = TotalOccurances_array[EqnCounter, SNCounter]

                        MAINtable_array_temp[0, 10] = ConsecutiveLegs_array[EqnCounter, SNCounter]
                        MAINtable_array_temp[0, 11] = IntermittentInLeg_array[EqnCounter, SNCounter]
                        MAINtable_array_temp[0, 12] = Flags_array[EqnCounter, SNCounter]

                        # if the input is empty set the priority to 4
                        if MDCMessagesArray[EqnCounter, 15] == 0:
                            MAINtable_array_temp[0, 13] = 4
                        else:
                            MAINtable_array_temp[0, 13] = MDCMessagesArray[EqnCounter, 15]

                        # For B1-006424 & B1-006430 Could MDC Trend tool assign Priority 3 if logged on A/C below 10340, 15317. Priority 1 if logged on 10340, 15317, 19001 and up
                        if MDCMessagesArray[EqnCounter, 12] == "B1-006424" or MDCMessagesArray[
                            EqnCounter, 12] == "B1-006430":
                            if int(UniqueSerialNumArray[SNCounter]) <= 10340 and int(
                                    UniqueSerialNumArray[SNCounter]) > 10000:
                                MAINtable_array_temp[0, 13] = 3
                            elif int(UniqueSerialNumArray[SNCounter]) > 10340 and int(
                                    UniqueSerialNumArray[SNCounter]) < 11000:
                                MAINtable_array_temp[0, 13] = 1
                            elif int(UniqueSerialNumArray[SNCounter]) <= 15317 and int(
                                    UniqueSerialNumArray[SNCounter]) > 15000:
                                MAINtable_array_temp[0, 13] = 3
                            elif int(UniqueSerialNumArray[SNCounter]) > 15317 and int(
                                    UniqueSerialNumArray[SNCounter]) < 16000:
                                MAINtable_array_temp[0, 13] = 1
                            elif int(UniqueSerialNumArray[SNCounter]) >= 19001 and int(
                                    UniqueSerialNumArray[SNCounter]) < 20000:
                                MAINtable_array_temp[0, 13] = 1

                                # check the content of MHIRJ ISE recommendation and add to array
                        if MDCMessagesArray[EqnCounter, 16] == 0:
                            MAINtable_array_temp[0, 15] = ""
                        else:
                            MAINtable_array_temp[0, 15] = MDCMessagesArray[EqnCounter, 16]

                        # check content of "additional"
                        if MDCMessagesArray[EqnCounter, 17] == 0:
                            MAINtable_array_temp[0, 16] = ""
                        else:
                            MAINtable_array_temp[0, 16] = MDCMessagesArray[EqnCounter, 17]

                        # check content of "MHIRJ Input"
                        if MDCMessagesArray[EqnCounter, 18] == 0:
                            MAINtable_array_temp[0, 17] = ""
                        else:
                            MAINtable_array_temp[0, 17] = MDCMessagesArray[EqnCounter, 18]

                        # Check for the equation in the Top Messages sheet
                        TopCounter = 0
                        Top_LastRow = TopMessagesArray.shape[0]
                        while TopCounter < Top_LastRow:

                            # Look for the flagged equation in the Top Messages Sheet
                            if MDCMessagesArray[EqnCounter][12] == TopMessagesArray[TopCounter, 4]:

                                # Found the equation in the Top Messages Sheet. Put the information in the last column
                                MAINtable_array_temp[0, 14] = "Known Nuissance: " + str(
                                    TopMessagesArray[TopCounter, 13]) \
                                                              + " / In-Service Document: " + str(
                                    TopMessagesArray[TopCounter, 11]) \
                                                              + " / FIM Task: " + str(TopMessagesArray[TopCounter, 10]) \
                                                              + " / Remarks: " + str(TopMessagesArray[TopCounter, 14])

                                # Not need to keep looking
                                TopCounter = TopMessagesArray.shape[0]

                            else:
                                # Not equal, go to next equation
                                MAINtable_array_temp[0, 14] = ""
                                TopCounter += 1
                        # End while

                        if currentRow == 0:
                            MAINtable_array = np.array(MAINtable_array_temp)
                        else:
                            MAINtable_array = np.append(MAINtable_array, MAINtable_array_temp, axis=0)
                        # End if Build MAINtable_array

                        # Move to next Row on Main page for next flag
                        currentRow = currentRow + 1

        TitlesArrayDaily = ["Date", "AC SN", "EICAS Message", "MDC Message", "LRU", "ATA", "B1-Equation", "Type",
                            "Equation Description", "Total Occurences", "Consecutive FL",
                            "Intermittent", "Reason(s) for flag", "Priority",
                            "Known Top Message - Recommended Documents",
                            "MHIRJ ISE Recommendation", "Additional Comments", "MHIRJ ISE Input"]
        # Converts the Numpy Array to Dataframe to manipulate
        # pd.set_option('display.max_rows', None)
        OutputTableDaily = pd.DataFrame(data=MAINtable_array, columns=TitlesArrayDaily).fillna(" ").sort_values(
            by=["Date", "Type", "Priority"])
        OutputTableDaily = OutputTableDaily.merge(AircraftTailPairDF, on="AC SN")  # Tail # added
        OutputTableDaily = OutputTableDaily[
            ["Tail#", "Date", "AC SN", "EICAS Message", "MDC Message", "LRU", "ATA", "B1-Equation", "Type",
             "Equation Description", "Total Occurences", "Consecutive FL",
             "Intermittent", "Reason(s) for flag", "Priority", "Known Top Message - Recommended Documents",
             "MHIRJ ISE Recommendation", "Additional Comments",
             "MHIRJ ISE Input"]]  # Tail# added to output table which means that column order has to be re ordered

        #RelatedtoFlagged = flagsinreport(OutputTable=OutputTableDaily, Aircraft=ACSN_chosen)
        #print(RelatedtoFlagged)

        #listofJamMessages = list()
        all_jam_messages = connect_to_fetch_all_jam_messages()
        for each_jam_message in all_jam_messages['Jam_Message']:
            listofJamMessages.append(each_jam_message)
        # print(listofJamMessages)
        OutputTable = OutputTableDaily
        # print(OutputTable)
        datatofilter = MDCdataDF.copy(deep=True)
        # print(datatofilter)
        isin = OutputTable["B1-Equation"].isin(listofJamMessages)
        filter1 = OutputTable[isin][["AC SN", "B1-Equation"]]
        listoftuplesACID = list(zip(filter1["AC SN"], filter1["B1-Equation"]))
        print("listoftuplesACID :::")
        print(listoftuplesACID)
        datatofilter2 = datatofilter.set_index(["Aircraft", "Equation ID"]).sort_index().loc[
                        pd.IndexSlice[listoftuplesACID], :].reset_index()
        listoftuplesACFL = list(zip(datatofilter2["Aircraft"], datatofilter2["Flight Leg No"]))
        print("datatofilter2 :::")
        print(datatofilter2)

        datatofilter3 = datatofilter.set_index(["Aircraft", "Flight Leg No"]).sort_index()
        FinalDF = datatofilter3.loc[pd.IndexSlice[listoftuplesACFL], :]
        print("\n---datatofilter3 :::")
        print(datatofilter3)

        print("\n----FinalDF :::")
        print(FinalDF)
        # return FinalDF.loc[ACSN_chosen]
        print("\n----ACSN_chosen :::")
        print(ACSN_chosen)
        print("\n----ACSN_chosen 2:::")
        print(FinalDF.loc[str(ACSN_chosen)])
        
        FinalDF_daily_json = (FinalDF.loc[str(ACSN_chosen)]).to_json(orient='records')
        return FinalDF_daily_json

    elif (analysisType == "history"):

        HistoryanalysisDF = MDCdataDF
        ShapeHistoryanalysisDF = HistoryanalysisDF.shape  # tuple of the shape of the history data (#rows, #columns)
        HistoryanalysisArray = MDCdataArray
        NumAC = HistoryanalysisDF["Aircraft"].nunique()  # number of unique aircraft SN in the data
        UniqueSerialNumArray = HistoryanalysisDF.Aircraft.unique()  # unique aircraft values
        SerialNumFreqSeries = HistoryanalysisDF.Aircraft.value_counts()  # the index of this var contains the AC with the most occurrences
        MaxOfAnAC = SerialNumFreqSeries[0]  # the freq series sorts in descending order, max value is top

        # Define the arrays as numpy
        MDCeqns_array = np.empty((MaxOfAnAC, NumAC), object)  # MDC messages for each AC stored in one array
        MDCDates_array = np.empty((MaxOfAnAC, NumAC), object)  # Dates for a message for each AC stored in one array
        MDCLegs_array = np.empty((MaxOfAnAC, NumAC),
                                 object)  # Flight Legs for a message for each AC stored in one array
        MDCIntermittent_array = np.empty((MaxOfAnAC, NumAC),
                                         object)  # stores the intermittence values for each message of each array
        FourDigATA_array = np.empty((MaxOfAnAC, NumAC), object)  # stores the 4digATAs of each message in one array
        TwoDigATA_array = np.empty((MaxOfAnAC, NumAC), object)  # stores the 2digATAs of each message in one array
        global MDCeqns_arrayforgraphing
        MDCeqns_arrayforgraphing = np.empty((MaxOfAnAC, NumAC),
                                            object)  # MDC messages for each AC stored in an array for graphing, due to current messages issue

        if CurrentFlightPhaseEnabled == 1:  # Show all, current and history
            MDCFlightPhase_array = np.ones((MaxOfAnAC, NumAC), int)
        elif CurrentFlightPhaseEnabled == 0:  # Only show history
            MDCFlightPhase_array = np.empty((MaxOfAnAC, NumAC), object)

        Messages_LastRow = ShapeMDCMessagesArray[0]  # taken from the shape of the array
        Flags_array = np.empty((Messages_LastRow, NumAC), object)
        FlightLegsEx = 'Flight legs above 32,600 for the following A/C: '  # at 32767 the DCU does not incrementmore the flight counter, so the MDC gets data for the same 32767 over and over until the limit of MDC logs per flight leg is reached (20 msgs per leg), when reached the MDC stops storing data since it gets always the same 32767
        TotalOccurances_array = np.empty((Messages_LastRow, NumAC), int)
        ConsecutiveDays_array = np.empty((Messages_LastRow, NumAC), int)
        ConsecutiveLegs_array = np.empty((Messages_LastRow, NumAC), int)
        IntermittentInLeg_array = np.empty((Messages_LastRow, NumAC), int)

        # 2D array looping, columns (SNcounter) rows (MDCsheetcounter)
        for SNCounter in range(0, NumAC):  # start counter over each aircraft (columns)

            MDCArrayCounter = 0  # rows of each different array

            for MDCsheetCounter in range(0, ShapeHistoryanalysisDF[0]):  # counter over each entry  (rows)

                # If The Serial number on the historyanalysisarray matches the current Serial Number, copy
                if HistoryanalysisArray[MDCsheetCounter, 0] == UniqueSerialNumArray[SNCounter]:
                    # Serial numbers match, record information
                    #       SNcounter -->
                    # format for these arrays :   | AC1 | AC2 | AC3 |.... | NumAC
                    # MDCarraycounter(vertically)| xx | xx | xx |...
                    MDCeqns_array[MDCArrayCounter, SNCounter] = HistoryanalysisArray[MDCsheetCounter, 14]
                    MDCDates_array[MDCArrayCounter, SNCounter] = HistoryanalysisArray[MDCsheetCounter, 8]
                    MDCLegs_array[MDCArrayCounter, SNCounter] = HistoryanalysisArray[MDCsheetCounter, 2]
                    MDCIntermittent_array[MDCArrayCounter, SNCounter] = HistoryanalysisArray[MDCsheetCounter, 13]

                    if HistoryanalysisArray[MDCsheetCounter, 11]:  # populating counts array
                        FourDigATA_array[MDCArrayCounter, SNCounter] = HistoryanalysisArray[MDCsheetCounter, 5]
                        TwoDigATA_array[MDCArrayCounter, SNCounter] = HistoryanalysisArray[MDCsheetCounter, 3]

                    if CurrentFlightPhaseEnabled == 0:  # populating the empty array
                        MDCFlightPhase_array[MDCArrayCounter, SNCounter] = HistoryanalysisArray[MDCsheetCounter, 11]

                    MDCArrayCounter = MDCArrayCounter + 1

            # arrays with the same size as the MDC messages sheet (3519) checks if each message exists in each ac
            for MessagessheetCounter in range(0, Messages_LastRow):

                # Initialize Counts, etc

                # Total Occurances
                eqnCount = 0

                # Consecutive Days
                ConsecutiveDays = 0
                MaxConsecutiveDays = 0
                tempDate = pd.to_datetime(latestDay)
                DaysCount = 0

                # Consecutive Legs
                ConsecutiveLegs = 0
                MaxConsecutiveLegs = 0
                tempLeg = LastLeg

                # Intermittent
                IntermittentFlightLegs = 0

                MDCArrayCounter = 0

                while MDCArrayCounter < MaxOfAnAC:
                    if MDCeqns_array[MDCArrayCounter, SNCounter]:
                        # Not Empty, and not current                                      B code
                        if MDCeqns_array[MDCArrayCounter, SNCounter] == MDCMessagesArray[MessagessheetCounter, 12] \
                                and MDCFlightPhase_array[MDCArrayCounter, SNCounter]:

                            MDCeqns_arrayforgraphing[MDCArrayCounter, SNCounter] = MDCeqns_array[
                                MDCArrayCounter, SNCounter]

                            # Total Occurances
                            # Count this as 1 occurance
                            eqnCount = eqnCount + 1

                            # Consecutive Days
                            currentdate = pd.to_datetime(MDCDates_array[MDCArrayCounter, SNCounter])
                            # first date and fivedaysafter
                            # if a flag was raised in the previous count, it has to be reset and a new fivedaysafter is declared
                            if eqnCount == 1 or flag == True:
                                flag = False
                                FiveDaysAfter = currentdate + datetime.timedelta(5)

                            # by checking when its even or odd, we can check if the message occurred twice in 5 days
                            # if the second time it occurred is below fivedaysafter, flag is true
                            # if the second time it occurred is greater than fivedaysafter, flag is false
                            # if eqncount is odd, flag is false and new fivedaysafter is declared
                            if (eqnCount % 2) == 0:
                                if currentdate <= FiveDaysAfter:
                                    flag = True
                                else:
                                    flag = False
                            else:
                                FiveDaysAfter = currentdate + datetime.timedelta(5)
                                flag = False

                            if currentdate.day == tempDate.day \
                                    and currentdate.month == tempDate.month \
                                    and currentdate.year == tempDate.year:

                                DaysCount = 1  # 1 because consecutive means 1 day since it occured
                                tempDate = tempDate - datetime.timedelta(1)
                                ConsecutiveDays = ConsecutiveDays + 1

                                if ConsecutiveDays >= MaxConsecutiveDays:
                                    MaxConsecutiveDays = ConsecutiveDays

                            elif MDCDates_array[MDCArrayCounter, SNCounter] < tempDate:

                                # If not consecutive, start over
                                if ConsecutiveDays >= MaxConsecutiveDays:
                                    MaxConsecutiveDays = ConsecutiveDays

                                ConsecutiveDays = 1
                                # Days count is the delta between this current date and the previous temp date
                                DaysCount += abs(tempDate - currentdate).days + 1
                                tempDate = currentdate - datetime.timedelta(1)

                            # Consecutive Legs
                            if MDCLegs_array[MDCArrayCounter, SNCounter] == tempLeg:

                                tempLeg = tempLeg - 1
                                ConsecutiveLegs = ConsecutiveLegs + 1

                                if ConsecutiveLegs > MaxConsecutiveLegs:
                                    MaxConsecutiveLegs = ConsecutiveLegs

                            else:

                                # If not consecutive, start over
                                ConsecutiveLegs = 1
                                tempLeg = MDCLegs_array[MDCArrayCounter, SNCounter]

                            # Intermittent
                            # Taking the maximum intermittent value
                            x = MDCIntermittent_array[MDCArrayCounter, SNCounter]
                            # if isinstance(x, numbers.Number) and MDCIntermittent_array[MDCArrayCounter, SNCounter] > IntermittentFlightLegs:
                            if MDCIntermittent_array[MDCArrayCounter, SNCounter] > IntermittentFlightLegs:
                                IntermittentFlightLegs = MDCIntermittent_array[MDCArrayCounter, SNCounter]
                            # End if Intermittent numeric check

                            # Other
                            # Check that the legs is not over the given limit
                            Flags_array[MessagessheetCounter, SNCounter] = ''
                            if MDCLegs_array[MDCArrayCounter, SNCounter] > 32600:
                                FlightLegsEx = FlightLegsEx + str(UniqueSerialNumArray[SNCounter]) + ' (' + str(
                                    MDCLegs_array[MDCArrayCounter, SNCounter]) + ')' + ' '
                            # End if Legs flag

                            # Check for Other Flags
                            if MDCMessagesArray[MessagessheetCounter, 13]:
                                # Immediate (occurrance flag in excel MDC Messages inputs sheet) - JAM RELATED FLAGS
                                if MDCMessagesArray[MessagessheetCounter, 13] == 1 and MDCMessagesArray[MessagessheetCounter, 14] == 0:
                                    # Immediate Flag required
                                    # lIST OF MESSAGES TO BE FLAGGED AS SOON AS POSTED
                                    # ["B1-309178","B1-309179","B1-309180","B1-060044","B1-060045","B1-007973",
                                    # "B1-060017","B1-006551","B1-240885","B1-006552","B1-006553","B1-006554",
                                    # "B1-006555","B1-007798","B1-007772","B1-240938","B1-007925","B1-007905",
                                    # "B1-007927","B1-007915","B1-007926","B1-007910","B1-007928","B1-007920"]
                                    Flags_array[MessagessheetCounter, SNCounter] = str(
                                        MDCMessagesArray[MessagessheetCounter, 12]) + " occured at least once."


                                elif MDCMessagesArray[MessagessheetCounter, 13] == 2 and \
                                        MDCMessagesArray[MessagessheetCounter, 14] == 5 and \
                                        flag == True:
                                    # Triggered twice in 5 days
                                    # "B1-008350","B1-008351","B1-008360","B1-008361"
                                    Flags_array[MessagessheetCounter, SNCounter] = str(MDCMessagesArray[
                                                                                           MessagessheetCounter, 12]) + " occured at least twice in 5 days. "
                        MDCArrayCounter += 1

                    else:
                        MDCArrayCounter = MaxOfAnAC

                        # Next MDCArray Counter

                TotalOccurances_array[MessagessheetCounter, SNCounter] = eqnCount
                ConsecutiveDays_array[MessagessheetCounter, SNCounter] = MaxConsecutiveDays
                ConsecutiveLegs_array[MessagessheetCounter, SNCounter] = MaxConsecutiveLegs
                IntermittentInLeg_array[MessagessheetCounter, SNCounter] = IntermittentFlightLegs
            # Next MessagessheetCounter
        # Next SNCounter

        MAINtable_array_temp = np.empty((1, 18), object)  # 18 because its history #????????
        currentRow = 0
        MAINtable_array = []
        for SNCounter in range(0, NumAC):
            for EqnCounter in range(0, Messages_LastRow):

                # Continue with Report
                if TotalOccurances_array[EqnCounter, SNCounter] >= MaxAllowedOccurances \
                        or ConsecutiveDays_array[EqnCounter, SNCounter] >= MaxAllowedConsecDays \
                        or ConsecutiveLegs_array[EqnCounter, SNCounter] >= MaxAllowedConsecLegs \
                        or IntermittentInLeg_array[EqnCounter, SNCounter] >= MaxAllowedIntermittent \
                        or Flags_array[EqnCounter, SNCounter]:

                    # Populate Flags Array
                    if TotalOccurances_array[EqnCounter, SNCounter] >= MaxAllowedOccurances:
                        Flags_array[EqnCounter, SNCounter] = Flags_array[
                                                                 EqnCounter, SNCounter] + "Total occurances exceeded " + str(
                            MaxAllowedOccurances) + " occurances. "

                    if ConsecutiveDays_array[EqnCounter, SNCounter] >= MaxAllowedConsecDays:
                        Flags_array[EqnCounter, SNCounter] = Flags_array[
                                                                 EqnCounter, SNCounter] + "Maximum consecutive days exceeded " + str(
                            MaxAllowedConsecDays) + " days. "

                    if ConsecutiveLegs_array[EqnCounter, SNCounter] >= MaxAllowedConsecLegs:
                        Flags_array[EqnCounter, SNCounter] = Flags_array[
                                                                 EqnCounter, SNCounter] + "Maximum consecutive flight legs exceeded " + str(
                            MaxAllowedConsecLegs) + " flight legs. "

                    if IntermittentInLeg_array[EqnCounter, SNCounter] >= MaxAllowedIntermittent:
                        Flags_array[EqnCounter, SNCounter] = Flags_array[
                                                                 EqnCounter, SNCounter] + "Maximum intermittent occurances for one flight leg exceeded " + str(
                            MaxAllowedIntermittent) + " occurances. "

                    # populating the final array (Table)
                    MAINtable_array_temp[0, 0] = UniqueSerialNumArray[SNCounter]
                    MAINtable_array_temp[0, 1] = MDCMessagesArray[EqnCounter, 8]
                    MAINtable_array_temp[0, 2] = MDCMessagesArray[EqnCounter, 4]
                    MAINtable_array_temp[0, 3] = MDCMessagesArray[EqnCounter, 0]
                    MAINtable_array_temp[0, 4] = MDCMessagesArray[EqnCounter, 1]
                    MAINtable_array_temp[0, 5] = MDCMessagesArray[EqnCounter, 12]
                    MAINtable_array_temp[0, 6] = MDCMessagesArray[EqnCounter, 7]
                    MAINtable_array_temp[0, 7] = MDCMessagesArray[EqnCounter, 11]
                    MAINtable_array_temp[0, 8] = TotalOccurances_array[EqnCounter, SNCounter]
                    MAINtable_array_temp[0, 9] = ConsecutiveDays_array[EqnCounter, SNCounter]
                    MAINtable_array_temp[0, 10] = ConsecutiveLegs_array[EqnCounter, SNCounter]
                    MAINtable_array_temp[0, 11] = IntermittentInLeg_array[EqnCounter, SNCounter]
                    MAINtable_array_temp[0, 12] = Flags_array[EqnCounter, SNCounter]

                    # if the input is empty set the priority to 4
                    if MDCMessagesArray[EqnCounter, 15] == 0:
                        MAINtable_array_temp[0, 13] = 4
                    else:
                        MAINtable_array_temp[0, 13] = MDCMessagesArray[EqnCounter, 15]

                    # For B1-006424 & B1-006430 Could MDC Trend tool assign Priority 3 if logged on A/C below 10340, 15317. Priority 1 if logged on 10340, 15317, 19001 and up
                    if MDCMessagesArray[EqnCounter, 12] == "B1-006424" or MDCMessagesArray[
                        EqnCounter, 12] == "B1-006430":
                        if int(UniqueSerialNumArray[SNCounter]) <= 10340 and int(
                                UniqueSerialNumArray[SNCounter]) > 10000:
                            MAINtable_array_temp[0, 13] = 3
                        elif int(UniqueSerialNumArray[SNCounter]) > 10340 and int(
                                UniqueSerialNumArray[SNCounter]) < 11000:
                            MAINtable_array_temp[0, 13] = 1
                        elif int(UniqueSerialNumArray[SNCounter]) <= 15317 and int(
                                UniqueSerialNumArray[SNCounter]) > 15000:
                            MAINtable_array_temp[0, 13] = 3
                        elif int(UniqueSerialNumArray[SNCounter]) > 15317 and int(
                                UniqueSerialNumArray[SNCounter]) < 16000:
                            MAINtable_array_temp[0, 13] = 1
                        elif int(UniqueSerialNumArray[SNCounter]) >= 19001 and int(
                                UniqueSerialNumArray[SNCounter]) < 20000:
                            MAINtable_array_temp[0, 13] = 1

                    # check the content of MHIRJ ISE recommendation and add to array
                    if MDCMessagesArray[EqnCounter, 16] == 0:
                        MAINtable_array_temp[0, 15] = ""
                    else:
                        MAINtable_array_temp[0, 15] = MDCMessagesArray[EqnCounter, 16]

                    # check content of "additional"
                    if MDCMessagesArray[EqnCounter, 17] == 0:
                        MAINtable_array_temp[0, 16] = ""
                    else:
                        MAINtable_array_temp[0, 16] = MDCMessagesArray[EqnCounter, 17]

                    # check content of "MHIRJ Input"
                    if MDCMessagesArray[EqnCounter, 18] == 0:
                        MAINtable_array_temp[0, 17] = ""
                    else:
                        MAINtable_array_temp[0, 17] = MDCMessagesArray[EqnCounter, 18]

                    # Check for the equation in the Top Messages sheet
                    TopCounter = 0
                    Top_LastRow = TopMessagesArray.shape[0]
                    while TopCounter < Top_LastRow:

                        # Look for the flagged equation in the Top Messages Sheet
                        if MDCMessagesArray[EqnCounter][12] == TopMessagesArray[TopCounter, 4]:

                            # Found the equation in the Top Messages Sheet. Put the information in the last column
                            MAINtable_array_temp[0, 14] = "Known Nuissance: " + str(TopMessagesArray[TopCounter, 13]) \
                                                          + " / In-Service Document: " + str(
                                TopMessagesArray[TopCounter, 11]) \
                                                          + " / FIM Task: " + str(TopMessagesArray[TopCounter, 10]) \
                                                          + " / Remarks: " + str(TopMessagesArray[TopCounter, 14])

                            # Not need to keep looking
                            TopCounter = TopMessagesArray.shape[0]

                        else:
                            # Not equal, go to next equation
                            MAINtable_array_temp[0, 14] = ""
                            TopCounter += 1
                    # End while

                    if currentRow == 0:
                        MAINtable_array = np.array(MAINtable_array_temp)
                    else:
                        MAINtable_array = np.append(MAINtable_array, MAINtable_array_temp, axis=0)
                    # End if Build MAINtable_array

                    # Move to next Row on Main page for next flag
                    currentRow = currentRow + 1
        TitlesArrayHistory = ["AC SN", "EICAS Message", "MDC Message", "LRU", "ATA", "B1-Equation", "Type",
                              "Equation Description", "Total Occurences", "Consective Days", "Consecutive FL",
                              "Intermittent", "Reason(s) for flag", "Priority",
                              "Known Top Message - Recommended Documents",
                              "MHIRJ ISE Recommendation", "Additional Comments", "MHIRJ ISE Input"]

        # Converts the Numpy Array to Dataframe to manipulate
        # pd.set_option('display.max_rows', None)
        # Main table
        global OutputTableHistory
        OutputTableHistory = pd.DataFrame(data=MAINtable_array, columns=TitlesArrayHistory).fillna(" ").sort_values(
            by=["Type", "Priority"])
        OutputTableHistory = OutputTableHistory.merge(AircraftTailPairDF, on="AC SN")  # Tail # added
        OutputTableHistory = OutputTableHistory[
            ["Tail#", "AC SN", "EICAS Message", "MDC Message", "LRU", "ATA", "B1-Equation", "Type",
             "Equation Description", "Total Occurences", "Consective Days", "Consecutive FL",
             "Intermittent", "Reason(s) for flag", "Priority", "Known Top Message - Recommended Documents",
             "MHIRJ ISE Recommendation", "Additional Comments",
             "MHIRJ ISE Input"]]  # Tail# added to output table which means that column order has to be re ordered

        #RelatedtoFlagged = flagsinreport(OutputTable=OutputTableDaily, Aircraft=ACSN_chosen)
        #print(RelatedtoFlagged)

        #listofJamMessages = list()
        all_jam_messages = connect_to_fetch_all_jam_messages()
        for each_jam_message in all_jam_messages['Jam_Message']:
            listofJamMessages.append(each_jam_message)
        print(listofJamMessages)
        #OutputTable = OutputTableDaily

        datatofilter = MDCdataDF.copy(deep=True)
        # print(datatofilter)
        isin = OutputTableHistory["B1-Equation"].isin(listofJamMessages)
        filter1 = OutputTableHistory[isin][["AC SN", "B1-Equation"]]
        listoftuplesACID = list(zip(filter1["AC SN"], filter1["B1-Equation"]))

        datatofilter2 = datatofilter.set_index(["Aircraft", "Equation ID"]).sort_index().loc[
                        pd.IndexSlice[listoftuplesACID], :].reset_index()
        listoftuplesACFL = list(zip(datatofilter2["Aircraft"], datatofilter2["Flight Leg No"]))

        datatofilter3 = datatofilter.set_index(["Aircraft", "Flight Leg No"]).sort_index()
        FinalDF = datatofilter3.loc[pd.IndexSlice[listoftuplesACFL], :]
        print(FinalDF)
        #return FinalDF.loc[ACSN_chosen]
        FinalDF_history_json = (FinalDF.loc[str(ACSN_chosen)]).to_json(orient='records')
        return FinalDF_history_json


# Flags Report

def GetDates(Flagreport, DatesDF, Listoftuples, maxormin):
    '''
    This function runs inside the Toreport function to obtain a List (series) of minimum or maximum dates found in the data
    for any combination of AC SN and B1-code.
    Inputs:
    Flagreport: The report being made, or "Report" in the "Toreport" function
    DatesDF: A dataframe of Dates, B1-codes and AC SNs. Should be already filtered for current messages. Is done inside "Toreport" function
    Listoftuples: A list of pairs of AC SN and B1-codes. Same one as passed into Toreport function
    maxormin: Keywords "min" or "max". "min" returns a list of minimum dates found in data
                and "max" returns a list of maximum dates found in data

    '''
    List = []
    counts = pd.DataFrame(data=DatesDF.groupby(['Aircraft', "Equation ID", "DateAndTime"]).agg(len), columns=["Counts"])
    if maxormin == min:
        for x in Listoftuples:
            DatesfoundinMDCdata = counts.loc[(x[0], x[1])].resample('D')["Counts"].sum().index
            List.append(DatesfoundinMDCdata.min().date())

    elif maxormin == max:
        for x in Listoftuples:
            DatesfoundinMDCdata = counts.loc[(x[0], x[1])].resample('D')["Counts"].sum().index
            List.append(DatesfoundinMDCdata.max().date())

    return pd.Series(data=List)
from typing import Optional

# for reference -> http://localhost:8000/GenerateReport/history/2/2/2/8/('32','22')/('B1-007553', 'B1-246748')/skw/1/2020-11-11/2020-11-12/('10222','B1-006989'), ('10222','B1-007028'), ('10145','B1-007008')
#for Daily Report: value of consecutiveDays = 0 in URL -> for reference!!       ('32','22')/('B1-007553', 'B1-246748')/skw/1/2020-11-11/2020-11-12
@app.post("/api/GenerateReport/{analysisType}/{occurences}/{legs}/{intermittent}/{consecutiveDays}/{ata}/{exclude_EqID}/{airline_operator}/{include_current_message}/{fromDate}/{toDate}/{flag}/{list_of_tuples_acsn_bcode}")
async def generateFlagReport(analysisType: str, occurences: int, legs: int, intermittent: int, consecutiveDays: int, ata: str, exclude_EqID:str, airline_operator: str, include_current_message: int, fromDate: str , toDate: str, flag:int, list_of_tuples_acsn_bcode):
    print(fromDate, " ", toDate)
    """
    OutputTableHistory_json = generateReport('history',occurences,legs,intermittent,consecutiveDays,ata,exclude_EqID,airline_operator,include_current_message,fromDate,toDate)
    OTH_dict = json.loads(OutputTableHistory_json)
    OutputTableHistory_df = pd.DataFrame.from_dict(OTH_dict,orient = 'records')
    print(OutputTableHistory_df)
    """
    MDCdataDF = connect_database_MDCdata(ata, exclude_EqID, airline_operator, include_current_message, fromDate, toDate)
    print(MDCdataDF)
    # Date formatting
    MDCdataDF["DateAndTime"] = pd.to_datetime(MDCdataDF["DateAndTime"])
    # print(MDCdataDF["DateAndTime"])
    MDCdataDF["Flight Leg No"].fillna(value=0.0, inplace=True)  # Null values preprocessing - if 0 = Currentflightphase
    # print(MDCdataDF["Flight Leg No"])
    MDCdataDF["Flight Phase"].fillna(False, inplace=True)  # NuCell values preprocessing for currentflightphase
    MDCdataDF["Intermittent"].fillna(value=-1, inplace=True)  # Null values preprocessing for currentflightphase
    MDCdataDF["Intermittent"].replace(to_replace=">", value="9",
                                      inplace=True)  # > represents greater than 8 Intermittent values
    MDCdataDF["Intermittent"] = MDCdataDF["Intermittent"].astype(int)  # cast type to int
    MDCdataDF["Aircraft"] = MDCdataDF["Aircraft"].str.replace('AC', '')
    MDCdataDF.fillna(value=" ", inplace=True)  # replacing all REMAINING null values to a blank string

    AircraftTailPairDF = MDCdataDF[["Aircraft", "Tail#"]].drop_duplicates(
        ignore_index=True)  # unique pairs of AC SN and Tail# for use in analysis
    AircraftTailPairDF.columns = ["AC SN", "Tail#"]  # re naming the columns to match History/Daily analysis output

    DatesinData = MDCdataDF["DateAndTime"].dt.date.unique()  # these are the dates in the data in Datetime format.
    NumberofDays = len(MDCdataDF["DateAndTime"].dt.date.unique())  # to pass into Daily analysis number of days in data
    latestDay = str(MDCdataDF.loc[0, "DateAndTime"].date())  # to pass into Daily analysis
    LastLeg = max(MDCdataDF["Flight Leg No"])  # Latest Leg in the data
    MDCdataArray = MDCdataDF.to_numpy()  # converting to numpy to work with arrays

    # MDCMessagesDF = pd.read_csv(MDCMessagesURL_path, encoding="utf8")  # bring messages and inputs into a Dataframe
    MDCMessagesDF = connect_database_MDCmessagesInputs()
    MDCMessagesArray = MDCMessagesDF.to_numpy()  # converting to numpy to work with arrays
    ShapeMDCMessagesArray = MDCMessagesArray.shape  # tuple of the shape of the MDC message data (#rows, #columns)

    # TopMessagesDF = pd.read_csv(TopMessagesURL_path)  # bring messages and inputs into a Dataframe
    TopMessagesDF = connect_database_TopMessagesSheet()
    TopMessagesArray = TopMessagesDF.to_numpy()  # converting to numpy to work with arrays

    UniqueSerialNumArray = []

    if (airline_operator.upper() == "SKW"):
        airline_id = 101
    global exclude_current_messages
    if (include_current_message == 1):
        ExcludeCurrentMessage = False
        CurrentFlightPhaseEnabled = 1
    else:
        ExcludeCurrentMessage = True
        CurrentFlightPhaseEnabled = 0  # 1 or 0, 1 includes current phase, 0 does not include current phase

    MaxAllowedOccurances = occurences  # flag for Total number of occurences -> T
    MaxAllowedConsecLegs = legs  # flag for consecutive legs -> CF
    if intermittent > 9:
        MaxAllowedIntermittent = 9  # flag for intermittent values ->IM
    else:
        MaxAllowedIntermittent = intermittent  # flag for intermittent values ->IM
    MaxAllowedConsecDays = consecutiveDays

    # Flags parameters
    Flagsreport = 1
    #AircraftSN = acsn
    #Bcode = bcode
    newreport = True
    print(type(list_of_tuples_acsn_bcode))
    #ListofTupleofSNBcode = list_of_tuples_acsn_bcode
    ListofTupleofSNBcode = []
    list_to_convert = (list_of_tuples_acsn_bcode).split(', ')
    print(list_to_convert)
    new_list = list(map(eval, list_to_convert))
    print(new_list)
    for each in new_list:
        print(each)
        ListofTupleofSNBcode.append(each)
    print(ListofTupleofSNBcode)
    ty = isinstance(ListofTupleofSNBcode, list)
    print(ty)
    #print(ListofTupleofSNBcode)

    if (analysisType.upper() == "HISTORY"):
        """
        HistoryanalysisDF = MDCdataDF
        ShapeHistoryanalysisDF = HistoryanalysisDF.shape  # tuple of the shape of the history data (#rows, #columns)
        HistoryanalysisArray = MDCdataArray
        NumAC = HistoryanalysisDF["Aircraft"].nunique()  # number of unique aircraft SN in the data
        UniqueSerialNumArray = HistoryanalysisDF.Aircraft.unique()  # unique aircraft values
        SerialNumFreqSeries = HistoryanalysisDF.Aircraft.value_counts()  # the index of this var contains the AC with the most occurrences
        MaxOfAnAC = SerialNumFreqSeries[0]  # the freq series sorts in descending order, max value is top

        # Define the arrays as numpy
        MDCeqns_array = np.empty((MaxOfAnAC, NumAC), object)  # MDC messages for each AC stored in one array
        MDCDates_array = np.empty((MaxOfAnAC, NumAC), object)  # Dates for a message for each AC stored in one array
        MDCLegs_array = np.empty((MaxOfAnAC, NumAC),
                                 object)  # Flight Legs for a message for each AC stored in one array
        MDCIntermittent_array = np.empty((MaxOfAnAC, NumAC),
                                         object)  # stores the intermittence values for each message of each array
        FourDigATA_array = np.empty((MaxOfAnAC, NumAC), object)  # stores the 4digATAs of each message in one array
        TwoDigATA_array = np.empty((MaxOfAnAC, NumAC), object)  # stores the 2digATAs of each message in one array
        global MDCeqns_arrayforgraphing
        MDCeqns_arrayforgraphing = np.empty((MaxOfAnAC, NumAC),
                                            object)  # MDC messages for each AC stored in an array for graphing, due to current messages issue

        if CurrentFlightPhaseEnabled == 1:  # Show all, current and history
            MDCFlightPhase_array = np.ones((MaxOfAnAC, NumAC), int)
        elif CurrentFlightPhaseEnabled == 0:  # Only show history
            MDCFlightPhase_array = np.empty((MaxOfAnAC, NumAC), object)

        Messages_LastRow = ShapeMDCMessagesArray[0]  # taken from the shape of the array
        Flags_array = np.empty((Messages_LastRow, NumAC), object)
        FlightLegsEx = 'Flight legs above 32,600 for the following A/C: '  # at 32767 the DCU does not incrementmore the flight counter, so the MDC gets data for the same 32767 over and over until the limit of MDC logs per flight leg is reached (20 msgs per leg), when reached the MDC stops storing data since it gets always the same 32767
        TotalOccurances_array = np.empty((Messages_LastRow, NumAC), int)
        ConsecutiveDays_array = np.empty((Messages_LastRow, NumAC), int)
        ConsecutiveLegs_array = np.empty((Messages_LastRow, NumAC), int)
        IntermittentInLeg_array = np.empty((Messages_LastRow, NumAC), int)

        # 2D array looping, columns (SNcounter) rows (MDCsheetcounter)
        for SNCounter in range(0, NumAC):  # start counter over each aircraft (columns)

            MDCArrayCounter = 0  # rows of each different array

            for MDCsheetCounter in range(0, ShapeHistoryanalysisDF[0]):  # counter over each entry  (rows)

                # If The Serial number on the historyanalysisarray matches the current Serial Number, copy
                if HistoryanalysisArray[MDCsheetCounter, 0] == UniqueSerialNumArray[SNCounter]:
                    # Serial numbers match, record information
                    #       SNcounter -->
                    # format for these arrays :   | AC1 | AC2 | AC3 |.... | NumAC
                    # MDCarraycounter(vertically)| xx | xx | xx |...
                    MDCeqns_array[MDCArrayCounter, SNCounter] = HistoryanalysisArray[MDCsheetCounter, 14]
                    MDCDates_array[MDCArrayCounter, SNCounter] = HistoryanalysisArray[MDCsheetCounter, 8]
                    MDCLegs_array[MDCArrayCounter, SNCounter] = HistoryanalysisArray[MDCsheetCounter, 2]
                    MDCIntermittent_array[MDCArrayCounter, SNCounter] = HistoryanalysisArray[MDCsheetCounter, 13]

                    if HistoryanalysisArray[MDCsheetCounter, 11]:  # populating counts array
                        FourDigATA_array[MDCArrayCounter, SNCounter] = HistoryanalysisArray[MDCsheetCounter, 5]
                        TwoDigATA_array[MDCArrayCounter, SNCounter] = HistoryanalysisArray[MDCsheetCounter, 3]

                    if CurrentFlightPhaseEnabled == 0:  # populating the empty array
                        MDCFlightPhase_array[MDCArrayCounter, SNCounter] = HistoryanalysisArray[MDCsheetCounter, 11]

                    MDCArrayCounter = MDCArrayCounter + 1

            # arrays with the same size as the MDC messages sheet (3519) checks if each message exists in each ac
            for MessagessheetCounter in range(0, Messages_LastRow):

                # Initialize Counts, etc

                # Total Occurances
                eqnCount = 0

                # Consecutive Days
                ConsecutiveDays = 0
                MaxConsecutiveDays = 0
                tempDate = pd.to_datetime(latestDay)
                DaysCount = 0

                # Consecutive Legs
                ConsecutiveLegs = 0
                MaxConsecutiveLegs = 0
                tempLeg = LastLeg

                # Intermittent
                IntermittentFlightLegs = 0

                MDCArrayCounter = 0

                while MDCArrayCounter < MaxOfAnAC:
                    if MDCeqns_array[MDCArrayCounter, SNCounter]:
                        # Not Empty, and not current                                      B code
                        if MDCeqns_array[MDCArrayCounter, SNCounter] == MDCMessagesArray[MessagessheetCounter, 12] \
                                and MDCFlightPhase_array[MDCArrayCounter, SNCounter]:

                            MDCeqns_arrayforgraphing[MDCArrayCounter, SNCounter] = MDCeqns_array[
                                MDCArrayCounter, SNCounter]

                            # Total Occurances
                            # Count this as 1 occurance
                            eqnCount = eqnCount + 1

                            # Consecutive Days
                            currentdate = pd.to_datetime(MDCDates_array[MDCArrayCounter, SNCounter])
                            if currentdate.day == tempDate.day \
                                    and currentdate.month == tempDate.month \
                                    and currentdate.year == tempDate.year:

                                DaysCount = 1  # 1 because consecutive means 1 day since it occured
                                tempDate = tempDate - datetime.timedelta(1)
                                ConsecutiveDays = ConsecutiveDays + 1

                                if ConsecutiveDays >= MaxConsecutiveDays:
                                    MaxConsecutiveDays = ConsecutiveDays

                            elif MDCDates_array[MDCArrayCounter, SNCounter] < tempDate:

                                # If not consecutive, start over
                                if ConsecutiveDays >= MaxConsecutiveDays:
                                    MaxConsecutiveDays = ConsecutiveDays

                                ConsecutiveDays = 1
                                # Days count is the delta between this current date and the previous temp date
                                DaysCount += abs(tempDate - currentdate).days + 1
                                tempDate = currentdate - datetime.timedelta(1)

                            # Consecutive Legs
                            if MDCLegs_array[MDCArrayCounter, SNCounter] == tempLeg:

                                tempLeg = tempLeg - 1
                                ConsecutiveLegs = ConsecutiveLegs + 1

                                if ConsecutiveLegs > MaxConsecutiveLegs:
                                    MaxConsecutiveLegs = ConsecutiveLegs

                            else:

                                # If not consecutive, start over
                                ConsecutiveLegs = 1
                                tempLeg = MDCLegs_array[MDCArrayCounter, SNCounter]

                            # Intermittent
                            # Taking the maximum intermittent value
                            x = MDCIntermittent_array[MDCArrayCounter, SNCounter]
                            if isinstance(x, numbers.Number) and MDCIntermittent_array[
                                MDCArrayCounter, SNCounter] > IntermittentFlightLegs:
                                IntermittentFlightLegs = MDCIntermittent_array[MDCArrayCounter, SNCounter]
                            # End if Intermittent numeric check

                            # Other
                            # Check that the legs is not over the given limit
                            Flags_array[MessagessheetCounter, SNCounter] = ''

                            # Error corrected here

                            if MDCLegs_array[MDCArrayCounter, SNCounter] > 32600:
                                FlightLegsEx = FlightLegsEx + str(UniqueSerialNumArray[SNCounter]) + ' (' + str(
                                    MDCLegs_array[MDCArrayCounter, SNCounter]) + ')' + ' '
                            # End if Legs flag

                            # Check for Other Flags
                            if MDCMessagesArray[MessagessheetCounter, 13]:
                                # Immediate (occurrance flag in excel MDC Messages sheet)
                                if MDCMessagesArray[MessagessheetCounter, 13] == 1:
                                    # Immediate Flag required
                                    Flags_array[MessagessheetCounter, SNCounter] = str(
                                        MDCMessagesArray[MessagessheetCounter, 12]) + " occured at least once."

                        MDCArrayCounter += 1

                    else:
                        MDCArrayCounter = MaxOfAnAC

                        # Next MDCArray Counter

                TotalOccurances_array[MessagessheetCounter, SNCounter] = eqnCount
                ConsecutiveDays_array[MessagessheetCounter, SNCounter] = MaxConsecutiveDays
                ConsecutiveLegs_array[MessagessheetCounter, SNCounter] = MaxConsecutiveLegs
                IntermittentInLeg_array[MessagessheetCounter, SNCounter] = IntermittentFlightLegs

            # Next MessagessheetCounter
        # Next SNCounter

        MAINtable_array_temp = np.empty((1, 18), object)  # 18 because its history #????????
        currentRow = 0
        MAINtable_array = []
        for SNCounter in range(0, NumAC):
            for EqnCounter in range(0, Messages_LastRow):

                # Continue with Report
                if TotalOccurances_array[EqnCounter, SNCounter] >= MaxAllowedOccurances \
                        or ConsecutiveDays_array[EqnCounter, SNCounter] >= MaxAllowedConsecDays \
                        or ConsecutiveLegs_array[EqnCounter, SNCounter] >= MaxAllowedConsecLegs \
                        or IntermittentInLeg_array[EqnCounter, SNCounter] >= MaxAllowedIntermittent \
                        or Flags_array[EqnCounter, SNCounter]:

                    # Populate Flags Array
                    if TotalOccurances_array[EqnCounter, SNCounter] >= MaxAllowedOccurances:
                        Flags_array[EqnCounter, SNCounter] = Flags_array[
                                                                 EqnCounter, SNCounter] + "Total occurances exceeded " + str(
                            MaxAllowedOccurances) + " occurances. "

                    if ConsecutiveDays_array[EqnCounter, SNCounter] >= MaxAllowedConsecDays:
                        Flags_array[EqnCounter, SNCounter] = Flags_array[
                                                                 EqnCounter, SNCounter] + "Maximum consecutive days exceeded " + str(
                            MaxAllowedConsecDays) + " days. "

                    if ConsecutiveLegs_array[EqnCounter, SNCounter] >= MaxAllowedConsecLegs:
                        Flags_array[EqnCounter, SNCounter] = Flags_array[
                                                                 EqnCounter, SNCounter] + "Maximum consecutive flight legs exceeded " + str(
                            MaxAllowedConsecLegs) + " flight legs. "

                    if IntermittentInLeg_array[EqnCounter, SNCounter] >= MaxAllowedIntermittent:
                        Flags_array[EqnCounter, SNCounter] = Flags_array[
                                                                 EqnCounter, SNCounter] + "Maximum intermittent occurances for one flight leg exceeded " + str(
                            MaxAllowedIntermittent) + " occurances. "

                    # populating the final array (Table)
                    MAINtable_array_temp[0, 0] = UniqueSerialNumArray[SNCounter]
                    MAINtable_array_temp[0, 1] = MDCMessagesArray[EqnCounter, 8]
                    MAINtable_array_temp[0, 2] = MDCMessagesArray[EqnCounter, 4]
                    MAINtable_array_temp[0, 3] = MDCMessagesArray[EqnCounter, 0]
                    MAINtable_array_temp[0, 4] = MDCMessagesArray[EqnCounter, 1]
                    MAINtable_array_temp[0, 5] = MDCMessagesArray[EqnCounter, 12]
                    MAINtable_array_temp[0, 6] = MDCMessagesArray[EqnCounter, 7]
                    MAINtable_array_temp[0, 7] = MDCMessagesArray[EqnCounter, 11]
                    MAINtable_array_temp[0, 8] = TotalOccurances_array[EqnCounter, SNCounter]
                    MAINtable_array_temp[0, 9] = ConsecutiveDays_array[EqnCounter, SNCounter]
                    MAINtable_array_temp[0, 10] = ConsecutiveLegs_array[EqnCounter, SNCounter]
                    MAINtable_array_temp[0, 11] = IntermittentInLeg_array[EqnCounter, SNCounter]
                    MAINtable_array_temp[0, 12] = Flags_array[EqnCounter, SNCounter]

                    # if the input is empty set the priority to 4
                    if MDCMessagesArray[EqnCounter, 15] == 0:
                        MAINtable_array_temp[0, 13] = 4
                    else:
                        MAINtable_array_temp[0, 13] = MDCMessagesArray[EqnCounter, 15]

                    # For B1-006424 & B1-006430 Could MDC Trend tool assign Priority 3 if logged on A/C below 10340, 15317. Priority 1 if logged on 10340, 15317, 19001 and up
                    if MDCMessagesArray[EqnCounter, 12] == "B1-006424" or MDCMessagesArray[
                        EqnCounter, 12] == "B1-006430":
                        if int(UniqueSerialNumArray[SNCounter]) <= 10340 and int(
                                UniqueSerialNumArray[SNCounter]) > 10000:
                            MAINtable_array_temp[0, 13] = 3
                        elif int(UniqueSerialNumArray[SNCounter]) > 10340 and int(
                                UniqueSerialNumArray[SNCounter]) < 11000:
                            MAINtable_array_temp[0, 13] = 1
                        elif int(UniqueSerialNumArray[SNCounter]) <= 15317 and int(
                                UniqueSerialNumArray[SNCounter]) > 15000:
                            MAINtable_array_temp[0, 13] = 3
                        elif int(UniqueSerialNumArray[SNCounter]) > 15317 and int(
                                UniqueSerialNumArray[SNCounter]) < 16000:
                            MAINtable_array_temp[0, 13] = 1
                        elif int(UniqueSerialNumArray[SNCounter]) >= 19001 and int(
                                UniqueSerialNumArray[SNCounter]) < 20000:
                            MAINtable_array_temp[0, 13] = 1

                    # check the content of MHIRJ ISE recommendation and add to array
                    if MDCMessagesArray[EqnCounter, 16] == 0:
                        MAINtable_array_temp[0, 15] = ""
                    else:
                        MAINtable_array_temp[0, 15] = MDCMessagesArray[EqnCounter, 16]

                    # check content of "additional"
                    if MDCMessagesArray[EqnCounter, 17] == 0:
                        MAINtable_array_temp[0, 16] = ""
                    else:
                        MAINtable_array_temp[0, 16] = MDCMessagesArray[EqnCounter, 17]

                    # check content of "MHIRJ Input"
                    if MDCMessagesArray[EqnCounter, 18] == 0:
                        MAINtable_array_temp[0, 17] = ""
                    else:
                        MAINtable_array_temp[0, 17] = MDCMessagesArray[EqnCounter, 18]

                    # Check for the equation in the Top Messages sheet
                    TopCounter = 0
                    Top_LastRow = TopMessagesArray.shape[0]
                    while TopCounter < Top_LastRow:

                        # Look for the flagged equation in the Top Messages Sheet
                        if MDCMessagesArray[EqnCounter][12] == TopMessagesArray[TopCounter, 4]:

                            # Found the equation in the Top Messages Sheet. Put the information in the last column
                            MAINtable_array_temp[0, 14] = "Known Nuissance: " + str(TopMessagesArray[TopCounter, 13]) \
                                                          + " / In-Service Document: " + str(
                                TopMessagesArray[TopCounter, 11]) \
                                                          + " / FIM Task: " + str(TopMessagesArray[TopCounter, 10]) \
                                                          + " / Remarks: " + str(TopMessagesArray[TopCounter, 14])

                            # Not need to keep looking
                            TopCounter = TopMessagesArray.shape[0]

                        else:
                            # Not equal, go to next equation
                            MAINtable_array_temp[0, 14] = ""
                            TopCounter += 1
                    # End while

                    if currentRow == 0:
                        MAINtable_array = np.array(MAINtable_array_temp)
                    else:
                        MAINtable_array = np.append(MAINtable_array, MAINtable_array_temp, axis=0)
                    # End if Build MAINtable_array

                    # Move to next Row on Main page for next flag
                    currentRow = currentRow + 1
        TitlesArrayHistory = ["AC SN", "EICAS Message", "MDC Message", "LRU", "ATA", "B1-Equation", "Type",
                              "Equation Description", "Total Occurences", "Consective Days", "Consecutive FL",
                              "Intermittent", "Reason(s) for flag", "Priority",
                              "Known Top Message - Recommended Documents",
                              "MHIRJ ISE Recommendation", "Additional Comments", "MHIRJ ISE Input"]
        """

        HistoryanalysisDF = MDCdataDF
        ShapeHistoryanalysisDF = HistoryanalysisDF.shape  # tuple of the shape of the history data (#rows, #columns)
        HistoryanalysisArray = MDCdataArray
        NumAC = HistoryanalysisDF["Aircraft"].nunique()  # number of unique aircraft SN in the data
        UniqueSerialNumArray = HistoryanalysisDF.Aircraft.unique()  # unique aircraft values
        SerialNumFreqSeries = HistoryanalysisDF.Aircraft.value_counts()  # the index of this var contains the AC with the most occurrences
        MaxOfAnAC = SerialNumFreqSeries[0]  # the freq series sorts in descending order, max value is top

        # Define the arrays as numpy
        MDCeqns_array = np.empty((MaxOfAnAC, NumAC), object)  # MDC messages for each AC stored in one array
        MDCDates_array = np.empty((MaxOfAnAC, NumAC), object)  # Dates for a message for each AC stored in one array
        MDCLegs_array = np.empty((MaxOfAnAC, NumAC),
                                 object)  # Flight Legs for a message for each AC stored in one array
        MDCIntermittent_array = np.empty((MaxOfAnAC, NumAC),
                                         object)  # stores the intermittence values for each message of each array
        FourDigATA_array = np.empty((MaxOfAnAC, NumAC), object)  # stores the 4digATAs of each message in one array
        TwoDigATA_array = np.empty((MaxOfAnAC, NumAC), object)  # stores the 2digATAs of each message in one array
        global MDCeqns_arrayforgraphing
        MDCeqns_arrayforgraphing = np.empty((MaxOfAnAC, NumAC),
                                            object)  # MDC messages for each AC stored in an array for graphing, due to current messages issue

        if CurrentFlightPhaseEnabled == 1:  # Show all, current and history
            MDCFlightPhase_array = np.ones((MaxOfAnAC, NumAC), int)
        elif CurrentFlightPhaseEnabled == 0:  # Only show history
            MDCFlightPhase_array = np.empty((MaxOfAnAC, NumAC), object)

        Messages_LastRow = ShapeMDCMessagesArray[0]  # taken from the shape of the array
        Flags_array = np.empty((Messages_LastRow, NumAC), object)
        FlightLegsEx = 'Flight legs above 32,600 for the following A/C: '  # at 32767 the DCU does not incrementmore the flight counter, so the MDC gets data for the same 32767 over and over until the limit of MDC logs per flight leg is reached (20 msgs per leg), when reached the MDC stops storing data since it gets always the same 32767
        TotalOccurances_array = np.empty((Messages_LastRow, NumAC), int)
        ConsecutiveDays_array = np.empty((Messages_LastRow, NumAC), int)
        ConsecutiveLegs_array = np.empty((Messages_LastRow, NumAC), int)
        IntermittentInLeg_array = np.empty((Messages_LastRow, NumAC), int)

        # 2D array looping, columns (SNcounter) rows (MDCsheetcounter)
        for SNCounter in range(0, NumAC):  # start counter over each aircraft (columns)

            MDCArrayCounter = 0  # rows of each different array

            for MDCsheetCounter in range(0, ShapeHistoryanalysisDF[0]):  # counter over each entry  (rows)

                # If The Serial number on the historyanalysisarray matches the current Serial Number, copy
                if HistoryanalysisArray[MDCsheetCounter, 0] == UniqueSerialNumArray[SNCounter]:
                    # Serial numbers match, record information
                    #       SNcounter -->
                    # format for these arrays :   | AC1 | AC2 | AC3 |.... | NumAC
                    # MDCarraycounter(vertically)| xx | xx | xx |...
                    MDCeqns_array[MDCArrayCounter, SNCounter] = HistoryanalysisArray[MDCsheetCounter, 14]
                    MDCDates_array[MDCArrayCounter, SNCounter] = HistoryanalysisArray[MDCsheetCounter, 8]
                    MDCLegs_array[MDCArrayCounter, SNCounter] = HistoryanalysisArray[MDCsheetCounter, 2]
                    MDCIntermittent_array[MDCArrayCounter, SNCounter] = HistoryanalysisArray[MDCsheetCounter, 13]

                    if HistoryanalysisArray[MDCsheetCounter, 11]:  # populating counts array
                        FourDigATA_array[MDCArrayCounter, SNCounter] = HistoryanalysisArray[MDCsheetCounter, 5]
                        TwoDigATA_array[MDCArrayCounter, SNCounter] = HistoryanalysisArray[MDCsheetCounter, 3]

                    if CurrentFlightPhaseEnabled == 0:  # populating the empty array
                        MDCFlightPhase_array[MDCArrayCounter, SNCounter] = HistoryanalysisArray[MDCsheetCounter, 11]

                    MDCArrayCounter = MDCArrayCounter + 1

            # arrays with the same size as the MDC messages sheet (3519) checks if each message exists in each ac
            for MessagessheetCounter in range(0, Messages_LastRow):

                # Initialize Counts, etc

                # Total Occurances
                eqnCount = 0

                # Consecutive Days
                ConsecutiveDays = 0
                MaxConsecutiveDays = 0
                tempDate = pd.to_datetime(latestDay)
                DaysCount = 0

                # Consecutive Legs
                ConsecutiveLegs = 0
                MaxConsecutiveLegs = 0
                tempLeg = LastLeg

                # Intermittent
                IntermittentFlightLegs = 0

                MDCArrayCounter = 0

                while MDCArrayCounter < MaxOfAnAC:
                    if MDCeqns_array[MDCArrayCounter, SNCounter]:
                        # Not Empty, and not current                                      B code
                        if MDCeqns_array[MDCArrayCounter, SNCounter] == MDCMessagesArray[MessagessheetCounter, 12] \
                                and MDCFlightPhase_array[MDCArrayCounter, SNCounter]:

                            MDCeqns_arrayforgraphing[MDCArrayCounter, SNCounter] = MDCeqns_array[
                                MDCArrayCounter, SNCounter]

                            # Total Occurances
                            # Count this as 1 occurance
                            eqnCount = eqnCount + 1

                            # Consecutive Days
                            currentdate = pd.to_datetime(MDCDates_array[MDCArrayCounter, SNCounter])
                            # first date and fivedaysafter
                            # if a flag was raised in the previous count, it has to be reset and a new fivedaysafter is declared
                            if eqnCount == 1 or flag == True:
                                flag = False
                                FiveDaysAfter = currentdate + datetime.timedelta(5)

                            # by checking when its even or odd, we can check if the message occurred twice in 5 days
                            # if the second time it occurred is below fivedaysafter, flag is true
                            # if the second time it occurred is greater than fivedaysafter, flag is false
                            # if eqncount is odd, flag is false and new fivedaysafter is declared
                            if (eqnCount % 2) == 0:
                                if currentdate <= FiveDaysAfter:
                                    flag = True
                                else:
                                    flag = False
                            else:
                                FiveDaysAfter = currentdate + datetime.timedelta(5)
                                flag = False

                            if currentdate.day == tempDate.day \
                                    and currentdate.month == tempDate.month \
                                    and currentdate.year == tempDate.year:

                                DaysCount = 1  # 1 because consecutive means 1 day since it occured
                                tempDate = tempDate - datetime.timedelta(1)
                                ConsecutiveDays = ConsecutiveDays + 1

                                if ConsecutiveDays >= MaxConsecutiveDays:
                                    MaxConsecutiveDays = ConsecutiveDays

                            elif MDCDates_array[MDCArrayCounter, SNCounter] < tempDate:

                                # If not consecutive, start over
                                if ConsecutiveDays >= MaxConsecutiveDays:
                                    MaxConsecutiveDays = ConsecutiveDays

                                ConsecutiveDays = 1
                                # Days count is the delta between this current date and the previous temp date
                                DaysCount += abs(tempDate - currentdate).days + 1
                                tempDate = currentdate - datetime.timedelta(1)

                            # Consecutive Legs
                            if MDCLegs_array[MDCArrayCounter, SNCounter] == tempLeg:

                                tempLeg = tempLeg - 1
                                ConsecutiveLegs = ConsecutiveLegs + 1

                                if ConsecutiveLegs > MaxConsecutiveLegs:
                                    MaxConsecutiveLegs = ConsecutiveLegs

                            else:

                                # If not consecutive, start over
                                ConsecutiveLegs = 1
                                tempLeg = MDCLegs_array[MDCArrayCounter, SNCounter]

                            # Intermittent
                            # Taking the maximum intermittent value
                            x = MDCIntermittent_array[MDCArrayCounter, SNCounter]
                            # if isinstance(x, numbers.Number) and MDCIntermittent_array[MDCArrayCounter, SNCounter] > IntermittentFlightLegs:
                            if MDCIntermittent_array[MDCArrayCounter, SNCounter] > IntermittentFlightLegs:
                                IntermittentFlightLegs = MDCIntermittent_array[MDCArrayCounter, SNCounter]
                            # End if Intermittent numeric check

                            # Other
                            # Check that the legs is not over the given limit
                            Flags_array[MessagessheetCounter, SNCounter] = ''
                            if MDCLegs_array[MDCArrayCounter, SNCounter] > 32600:
                                FlightLegsEx = FlightLegsEx + str(UniqueSerialNumArray[SNCounter]) + ' (' + str(
                                    MDCLegs_array[MDCArrayCounter, SNCounter]) + ')' + ' '
                            # End if Legs flag

                            # Check for Other Flags
                            if MDCMessagesArray[MessagessheetCounter, 13]:
                                # Immediate (occurrance flag in excel MDC Messages inputs sheet) - JAM RELATED FLAGS
                                if MDCMessagesArray[MessagessheetCounter, 13] == 1 and MDCMessagesArray[MessagessheetCounter, 14] == 0:
                                    # Immediate Flag required
                                    # lIST OF MESSAGES TO BE FLAGGED AS SOON AS POSTED
                                    # ["B1-309178","B1-309179","B1-309180","B1-060044","B1-060045","B1-007973",
                                    # "B1-060017","B1-006551","B1-240885","B1-006552","B1-006553","B1-006554",
                                    # "B1-006555","B1-007798","B1-007772","B1-240938","B1-007925","B1-007905",
                                    # "B1-007927","B1-007915","B1-007926","B1-007910","B1-007928","B1-007920"]
                                    Flags_array[MessagessheetCounter, SNCounter] = str(
                                        MDCMessagesArray[MessagessheetCounter, 12]) + " occured at least once."


                                elif MDCMessagesArray[MessagessheetCounter, 13] == 2 and \
                                        MDCMessagesArray[MessagessheetCounter, 14] == 5 and \
                                        flag == True:
                                    # Triggered twice in 5 days
                                    # "B1-008350","B1-008351","B1-008360","B1-008361"
                                    Flags_array[MessagessheetCounter, SNCounter] = str(MDCMessagesArray[
                                                                                           MessagessheetCounter, 12]) + " occured at least twice in 5 days. "
                        MDCArrayCounter += 1

                    else:
                        MDCArrayCounter = MaxOfAnAC

                        # Next MDCArray Counter

                TotalOccurances_array[MessagessheetCounter, SNCounter] = eqnCount
                ConsecutiveDays_array[MessagessheetCounter, SNCounter] = MaxConsecutiveDays
                ConsecutiveLegs_array[MessagessheetCounter, SNCounter] = MaxConsecutiveLegs
                IntermittentInLeg_array[MessagessheetCounter, SNCounter] = IntermittentFlightLegs
            # Next MessagessheetCounter
        # Next SNCounter

        MAINtable_array_temp = np.empty((1, 18), object)  # 18 because its history #????????
        currentRow = 0
        MAINtable_array = []
        for SNCounter in range(0, NumAC):
            for EqnCounter in range(0, Messages_LastRow):

                # Continue with Report
                if TotalOccurances_array[EqnCounter, SNCounter] >= MaxAllowedOccurances \
                        or ConsecutiveDays_array[EqnCounter, SNCounter] >= MaxAllowedConsecDays \
                        or ConsecutiveLegs_array[EqnCounter, SNCounter] >= MaxAllowedConsecLegs \
                        or IntermittentInLeg_array[EqnCounter, SNCounter] >= MaxAllowedIntermittent \
                        or Flags_array[EqnCounter, SNCounter]:

                    # Populate Flags Array
                    if TotalOccurances_array[EqnCounter, SNCounter] >= MaxAllowedOccurances:
                        Flags_array[EqnCounter, SNCounter] = Flags_array[
                                                                 EqnCounter, SNCounter] + "Total occurances exceeded " + str(
                            MaxAllowedOccurances) + " occurances. "

                    if ConsecutiveDays_array[EqnCounter, SNCounter] >= MaxAllowedConsecDays:
                        Flags_array[EqnCounter, SNCounter] = Flags_array[
                                                                 EqnCounter, SNCounter] + "Maximum consecutive days exceeded " + str(
                            MaxAllowedConsecDays) + " days. "

                    if ConsecutiveLegs_array[EqnCounter, SNCounter] >= MaxAllowedConsecLegs:
                        Flags_array[EqnCounter, SNCounter] = Flags_array[
                                                                 EqnCounter, SNCounter] + "Maximum consecutive flight legs exceeded " + str(
                            MaxAllowedConsecLegs) + " flight legs. "

                    if IntermittentInLeg_array[EqnCounter, SNCounter] >= MaxAllowedIntermittent:
                        Flags_array[EqnCounter, SNCounter] = Flags_array[
                                                                 EqnCounter, SNCounter] + "Maximum intermittent occurances for one flight leg exceeded " + str(
                            MaxAllowedIntermittent) + " occurances. "

                    # populating the final array (Table)
                    MAINtable_array_temp[0, 0] = UniqueSerialNumArray[SNCounter]
                    MAINtable_array_temp[0, 1] = MDCMessagesArray[EqnCounter, 8]
                    MAINtable_array_temp[0, 2] = MDCMessagesArray[EqnCounter, 4]
                    MAINtable_array_temp[0, 3] = MDCMessagesArray[EqnCounter, 0]
                    MAINtable_array_temp[0, 4] = MDCMessagesArray[EqnCounter, 1]
                    MAINtable_array_temp[0, 5] = MDCMessagesArray[EqnCounter, 12]
                    MAINtable_array_temp[0, 6] = MDCMessagesArray[EqnCounter, 7]
                    MAINtable_array_temp[0, 7] = MDCMessagesArray[EqnCounter, 11]
                    MAINtable_array_temp[0, 8] = TotalOccurances_array[EqnCounter, SNCounter]
                    MAINtable_array_temp[0, 9] = ConsecutiveDays_array[EqnCounter, SNCounter]
                    MAINtable_array_temp[0, 10] = ConsecutiveLegs_array[EqnCounter, SNCounter]
                    MAINtable_array_temp[0, 11] = IntermittentInLeg_array[EqnCounter, SNCounter]
                    MAINtable_array_temp[0, 12] = Flags_array[EqnCounter, SNCounter]

                    # if the input is empty set the priority to 4
                    if MDCMessagesArray[EqnCounter, 15] == 0:
                        MAINtable_array_temp[0, 13] = 4
                    else:
                        MAINtable_array_temp[0, 13] = MDCMessagesArray[EqnCounter, 15]

                    # For B1-006424 & B1-006430 Could MDC Trend tool assign Priority 3 if logged on A/C below 10340, 15317. Priority 1 if logged on 10340, 15317, 19001 and up
                    if MDCMessagesArray[EqnCounter, 12] == "B1-006424" or MDCMessagesArray[
                        EqnCounter, 12] == "B1-006430":
                        if int(UniqueSerialNumArray[SNCounter]) <= 10340 and int(
                                UniqueSerialNumArray[SNCounter]) > 10000:
                            MAINtable_array_temp[0, 13] = 3
                        elif int(UniqueSerialNumArray[SNCounter]) > 10340 and int(
                                UniqueSerialNumArray[SNCounter]) < 11000:
                            MAINtable_array_temp[0, 13] = 1
                        elif int(UniqueSerialNumArray[SNCounter]) <= 15317 and int(
                                UniqueSerialNumArray[SNCounter]) > 15000:
                            MAINtable_array_temp[0, 13] = 3
                        elif int(UniqueSerialNumArray[SNCounter]) > 15317 and int(
                                UniqueSerialNumArray[SNCounter]) < 16000:
                            MAINtable_array_temp[0, 13] = 1
                        elif int(UniqueSerialNumArray[SNCounter]) >= 19001 and int(
                                UniqueSerialNumArray[SNCounter]) < 20000:
                            MAINtable_array_temp[0, 13] = 1

                    # check the content of MHIRJ ISE recommendation and add to array
                    if MDCMessagesArray[EqnCounter, 16] == 0:
                        MAINtable_array_temp[0, 15] = ""
                    else:
                        MAINtable_array_temp[0, 15] = MDCMessagesArray[EqnCounter, 16]

                    # check content of "additional"
                    if MDCMessagesArray[EqnCounter, 17] == 0:
                        MAINtable_array_temp[0, 16] = ""
                    else:
                        MAINtable_array_temp[0, 16] = MDCMessagesArray[EqnCounter, 17]

                    # check content of "MHIRJ Input"
                    if MDCMessagesArray[EqnCounter, 18] == 0:
                        MAINtable_array_temp[0, 17] = ""
                    else:
                        MAINtable_array_temp[0, 17] = MDCMessagesArray[EqnCounter, 18]

                    # Check for the equation in the Top Messages sheet
                    TopCounter = 0
                    Top_LastRow = TopMessagesArray.shape[0]
                    while TopCounter < Top_LastRow:

                        # Look for the flagged equation in the Top Messages Sheet
                        if MDCMessagesArray[EqnCounter][12] == TopMessagesArray[TopCounter, 4]:

                            # Found the equation in the Top Messages Sheet. Put the information in the last column
                            MAINtable_array_temp[0, 14] = "Known Nuissance: " + str(TopMessagesArray[TopCounter, 13]) \
                                                          + " / In-Service Document: " + str(
                                TopMessagesArray[TopCounter, 11]) \
                                                          + " / FIM Task: " + str(TopMessagesArray[TopCounter, 10]) \
                                                          + " / Remarks: " + str(TopMessagesArray[TopCounter, 14])

                            # Not need to keep looking
                            TopCounter = TopMessagesArray.shape[0]

                        else:
                            # Not equal, go to next equation
                            MAINtable_array_temp[0, 14] = ""
                            TopCounter += 1
                    # End while

                    if currentRow == 0:
                        MAINtable_array = np.array(MAINtable_array_temp)
                    else:
                        MAINtable_array = np.append(MAINtable_array, MAINtable_array_temp, axis=0)
                    # End if Build MAINtable_array

                    # Move to next Row on Main page for next flag
                    currentRow = currentRow + 1
        TitlesArrayHistory = ["AC SN", "EICAS Message", "MDC Message", "LRU", "ATA", "B1-Equation", "Type",
                              "Equation Description", "Total Occurences", "Consective Days", "Consecutive FL",
                              "Intermittent", "Reason(s) for flag", "Priority",
                              "Known Top Message - Recommended Documents",
                              "MHIRJ ISE Recommendation", "Additional Comments", "MHIRJ ISE Input"]

        # Converts the Numpy Array to Dataframe to manipulate
        # pd.set_option('display.max_rows', None)
        # Main table
        global OutputTableHistory
        OutputTableHistory = pd.DataFrame(data=MAINtable_array, columns=TitlesArrayHistory).fillna(" ").sort_values(
            by=["Type", "Priority"])
        OutputTableHistory = OutputTableHistory.merge(AircraftTailPairDF, on="AC SN")  # Tail # added
        OutputTableHistory = OutputTableHistory[
            ["Tail#", "AC SN", "EICAS Message", "MDC Message", "LRU", "ATA", "B1-Equation", "Type",
             "Equation Description", "Total Occurences", "Consective Days", "Consecutive FL",
             "Intermittent", "Reason(s) for flag", "Priority", "Known Top Message - Recommended Documents",
             "MHIRJ ISE Recommendation", "Additional Comments",
             "MHIRJ ISE Input"]]  # Tail# added to output table which means that column order has to be re ordered
        # Converts the Numpy Array to Dataframe to manipulate
        # pd.set_option('display.max_rows', None)
        # Main table
        #global OutputTableHistory
        OutputTableHistory = pd.DataFrame(data=MAINtable_array, columns=TitlesArrayHistory).fillna(" ").sort_values(
            by=["Type", "Priority"])
        HistoryReport = OutputTableHistory

        ##### create a flags report #####
        DatesDF = MDCdataDF[["DateAndTime", "Equation ID", "Aircraft", "Flight Phase"]]

        if (analysisType.upper() == "HISTORY"):
            Report = HistoryReport

        #def Toreport(ListofTupleofSNBcode, DatesDF, ExcludeCurrentMessage, Report=OutputTableHistory):
        ''' Inputs:
            ListofTupleofSNBcode: Pass a list of tuples containing (Aircraft Serial Number, B1 Message code)
            in the form of (1XXXX, B1-XXXXXX)
            Report: Default parameter is OutputTableHistory from History Report, but can also obtain OutputTableDaily
            from Daily Report
            DatesDF: A DataFrame with the following columns from raw data ("DateAndTime","Equation ID", "Aircraft", "Flight Phase")
            ExcludeCurrentMessage: A boolean where True - Excludes current messages, False - Includes Current Messages

            Output:
            Report: A filtered and reorganized DataFrame containing the relevant information from the rows in the Passed report
        '''
        if ExcludeCurrentMessage == False:  # Show all, current and history
            #DatesDF['DateAndTime'] = DatesDF['DateAndTime'].astype(str).str.rstrip('00000')
            DatesDF = DatesDF[["DateAndTime", "Equation ID", "Aircraft"]].copy()

        elif ExcludeCurrentMessage == True:  # Only show history
            #DatesDF['DateAndTime'] = DatesDF['DateAndTime'].astype(str).str.rstrip('00000')
            DatesDF = DatesDF[["DateAndTime", "Equation ID", "Aircraft", "Flight Phase"]]
            # In the preprocessing for this Notebook, I changed NaNs to False.
            DatesDF = DatesDF.replace(False, np.nan).dropna(axis=0,
                                                            how='any')  # In the raw data, this is already blank. Could be a empty string or a NaN. Adjust according to raw data
            DatesDF = DatesDF[["DateAndTime", "Equation ID", "Aircraft"]].copy()
        print(DatesDF['DateAndTime'])
        Report = Report.set_index(["AC SN", "B1-Equation"]).sort_index()
        print(Report)

        Report = Report.loc[ListofTupleofSNBcode, ["ATA", "LRU", "MDC Message",
                                                   "Type", "EICAS Message", "MHIRJ ISE Input",
                                                   "MHIRJ ISE Recommendation"]]
        print(Report)
        Report = Report.rename(index={"AC SN": "MSN", "B1-Equation": "B1-Code"},
                               columns={"MDC Message": "Message",
                                        "EICAS Message": "Potential FDE",
                                        "MHIRJ ISE Input": "ISE Input",
                                        "MHIRJ ISE Recommendation": "ISE Rec Act"})
        Report.insert(loc=5, column="Date From", value=None)  # .date()removes the time data from datetime format
        Report.insert(loc=6, column="Date To", value=None)
        Report.insert(loc=7, column="SKW WIP", value="")
        Report = Report.reset_index()

        Report["Date From"] = GetDates(Report, DatesDF, ListofTupleofSNBcode, min)
        Report["Date To"] = GetDates(Report, DatesDF, ListofTupleofSNBcode, max)

        #return Report
        Flagsreport_json = Report.to_json(orient='records')
        return Flagsreport_json


##############################     CHARTS    ##############################

''' old code snippet till - 05/07/21
## Chart 1
def connect_database_for_chart1(n, aircraft_no, from_dt, to_dt):
    sql = "SELECT DISTINCT TOP "+str(n)+" Count(MDCMessagesInputs.Message) AS "'total_message'", Airline_MDC_Data. Equation_ID, MDCMessagesInputs.Message, MDCMessagesInputs.EICAS, Airline_MDC_Data.LRU, Airline_MDC_Data.ATA FROM Airline_MDC_Data INNER JOIN MDCMessagesInputs ON Airline_MDC_Data.ATA = MDCMessagesInputs.ATA AND Airline_MDC_Data.Equation_ID = MDCMessagesInputs.Equation_ID WHERE Airline_MDC_Data.aircraftno = "+str(aircraft_no)+" AND Airline_MDC_Data.DateAndTime BETWEEN '"+from_dt+"' AND '"+to_dt+"' GROUP BY Airline_MDC_Data.Equation_ID, MDCMessagesInputs.Message, MDCMessagesInputs.EICAS, Airline_MDC_Data.LRU, Airline_MDC_Data.ATA ORDER BY Count(MDCMessagesInputs.Message) DESC"

    try:
        conn = pyodbc.connect(driver=db_driver, host=hostname, database=db_name,
                              user=db_username, password=db_password)
        chart1_sql_df = pd.read_sql(sql, conn)
        #MDCdataDF.columns = column_names
        return chart1_sql_df
    except pyodbc.Error as err:
        print("Couldn't connect to Server")
        print("Error message:- " + str(err))


@app.post("/api/chart_one/{top_n}/{aircraftNo}/{fromDate}/{toDate}")
async def get_ChartOneData(top_n:int, aircraftNo:int, fromDate: str , toDate: str):
    chart1_sql_df = connect_database_for_chart1(top_n, aircraftNo, fromDate, toDate)
    chart1_sql_df_json = chart1_sql_df.to_json(orient='records')
    return chart1_sql_df_json
'''

## Chart 1
def connect_database_for_chart1(n, aircraft_no, ata_main, from_dt, to_dt):
    all_ata_str_list = []
    if ata_main == 'ALL':
        all_ata = connect_to_fetch_all_ata(from_dt, to_dt)

        all_ata_str = "("
        all_ata_list = all_ata['ATA_Main'].tolist()
        for each_ata in all_ata_list:
            all_ata_str_list.append(str(each_ata))
            all_ata_str += "'"+str(each_ata)+"'"
            if each_ata != all_ata_list[-1]:
                all_ata_str += ","
            else:
                all_ata_str += ")"
        print(all_ata_str)

    if ata_main == 'ALL':
        sql = "SELECT DISTINCT TOP "+str(n)+" Count(MDCMessagesInputs.Message) AS "'total_message'", Airline_MDC_Data. Equation_ID, MDCMessagesInputs.Message, MDCMessagesInputs.EICAS, Airline_MDC_Data.LRU, Airline_MDC_Data.ATA FROM Airline_MDC_Data INNER JOIN MDCMessagesInputs ON Airline_MDC_Data.ATA = MDCMessagesInputs.ATA AND Airline_MDC_Data.Equation_ID = MDCMessagesInputs.Equation_ID WHERE Airline_MDC_Data.aircraftno = "+str(aircraft_no)+" AND Airline_MDC_Data.ATA_Main IN " + str(all_ata_str) + " AND Airline_MDC_Data.DateAndTime BETWEEN '"+from_dt+"' AND '"+to_dt+"' GROUP BY Airline_MDC_Data.Equation_ID, MDCMessagesInputs.Message, MDCMessagesInputs.EICAS, Airline_MDC_Data.LRU, Airline_MDC_Data.ATA ORDER BY Count(MDCMessagesInputs.Message) DESC"
    else:
        sql = "SELECT DISTINCT TOP "+str(n)+" Count(MDCMessagesInputs.Message) AS "'total_message'", Airline_MDC_Data. Equation_ID, MDCMessagesInputs.Message, MDCMessagesInputs.EICAS, Airline_MDC_Data.LRU, Airline_MDC_Data.ATA FROM Airline_MDC_Data INNER JOIN MDCMessagesInputs ON Airline_MDC_Data.ATA = MDCMessagesInputs.ATA AND Airline_MDC_Data.Equation_ID = MDCMessagesInputs.Equation_ID WHERE Airline_MDC_Data.aircraftno = "+str(aircraft_no)+" AND Airline_MDC_Data.ATA_Main IN " + str(ata_main) + " AND Airline_MDC_Data.DateAndTime BETWEEN '"+from_dt+"' AND '"+to_dt+"' GROUP BY Airline_MDC_Data.Equation_ID, MDCMessagesInputs.Message, MDCMessagesInputs.EICAS, Airline_MDC_Data.LRU, Airline_MDC_Data.ATA ORDER BY Count(MDCMessagesInputs.Message) DESC"
    try:
        conn = pyodbc.connect(driver=db_driver, host=hostname, database=db_name,
                              user=db_username, password=db_password)
        chart1_sql_df = pd.read_sql(sql, conn)
        #MDCdataDF.columns = column_names
        return chart1_sql_df
    except pyodbc.Error as err:
        print("Couldn't connect to Server")
        print("Error message:- " + str(err))


@app.post("/api/chart_one/{top_n}/{aircraftNo}/{ata_main}/{fromDate}/{toDate}")
async def get_ChartOneData(top_n:int, aircraftNo:int, ata_main:str, fromDate: str , toDate: str):
    chart1_sql_df = connect_database_for_chart1(top_n, aircraftNo, ata_main, fromDate, toDate)
    chart1_sql_df_json = chart1_sql_df.to_json(orient='records')
    return chart1_sql_df_json

## Chart 2
def connect_database_for_chart2(n, ata, from_dt, to_dt):
   if len(ata) == 2:
        sql = "SELECT * FROM Airline_MDC_Data WHERE  ATA_Main="+ata+" AND DateAndTime BETWEEN '"+from_dt+"' AND '"+to_dt+"'"
   elif len(ata) == 5:  
       sql = "SELECT * FROM Airline_MDC_Data where  ATA='"+ata+"'   AND DateAndTime BETWEEN '"+from_dt+"' AND '"+to_dt+"' "
  
   column_names = ["Aircraft", "Tail", "Flight Leg No",
                   "ATA Main", "ATA Sub", "ATA", "ATA Description", "LRU",
                   "DateAndTime", "MDC Message", "Status", "Flight Phase", "Type",
                   "Intermittent", "Equation ID", "Source", "Diagnostic Data",
                   "Data Used to Determine Msg", "ID", "Flight", "airline_id", "aircraftno"]
   print(sql)
  
   try:
       conn = pyodbc.connect(driver=db_driver, host=hostname, database=db_name,
                             user=db_username, password=db_password)
       chart2_sql_df = pd.read_sql(sql, conn)
       # MDCdataDF.columns = column_names
       # chart2_sql_df.columns = column_names
 
       conn.close()
       return chart2_sql_df
   except pyodbc.Error as err:
       print("Couldn't connect to Server")
       print("Error message:- " + str(err))
 
@app.post("/api/chart_two/{top_values}/{ata}/{fromDate}/{toDate}")
async def get_ChartwoData(top_values:int, ata:str, fromDate: str , toDate: str):
  ATAtoStudy=ata
  Topvalues2=top_values
  MDCdataDF = connect_database_for_chart2(top_values, ata, fromDate, toDate)
  AircraftTailPairDF = MDCdataDF[["Aircraft", "Tail"]].drop_duplicates(ignore_index= True) # unique pairs of AC SN and Tail# for use in analysis
  AircraftTailPairDF.columns = ["AC SN","Tail"] # re naming the columns to match History/Daily analysis output
  chart2DF = pd.merge(left = MDCdataDF[["Aircraft","ATA_Main", "ATA"]], right = AircraftTailPairDF, left_on="Aircraft", right_on="AC SN")
  chart2DF["Aircraft"] = chart2DF["Aircraft"] + " / " + chart2DF["Tail"]
  chart2DF.drop(labels = ["AC SN", "Tail"], axis = 1, inplace = True)
  
  if len(ATAtoStudy) == 2:
     print(len(ATAtoStudy))
     # Convert 2 Dig ATA array to Dataframe to analyze
     TwoDigATA_DF = chart2DF.drop("ATA", axis = 1).copy()
     # Count the occurrence of each ata in each aircraft
     ATAOccurrenceDF = TwoDigATA_DF.value_counts().unstack()
     Plottinglabels = ATAOccurrenceDF[int(ATAtoStudy)].sort_values().dropna().tail(Topvalues2) # Aircraft Labels
    
  elif len(ATAtoStudy) == 5:
   # Convert 4 Dig ATA array to Dataframe to analyze
   FourDigATA_DF = chart2DF.drop("ATA_Main", axis = 1).copy()
   # Count the occurrence of each ata in each aircraft
   ATAOccurrenceDF = FourDigATA_DF.value_counts().unstack()
   Plottinglabels = ATAOccurrenceDF[ATAtoStudy].sort_values().dropna().tail(Topvalues2) # Aircraft Labels
   
  chart2_sql_df_json = Plottinglabels.to_json(orient='index')
  return chart2_sql_df_json




@app.post("/api/chart_three/{aircraft_no}/{equation_id}/{is_flight_phase_enabled}/{fromDate}/{toDate}")
async def get_CharThreeData(aircraft_no:int, equation_id:str, is_flight_phase_enabled:int, fromDate: str , toDate: str):
    return chart3Report(aircraft_no, equation_id, is_flight_phase_enabled, fromDate, toDate)


## Chart 5
def connect_database_for_chart5(aircraft_no, equation_id, is_flight_phase_enabled, from_dt, to_dt):
    if is_flight_phase_enabled == 0: # Flight phase is NOT enabled => exclude current message
        sql = "SELECT Intermittent AS OccurencesOfIntermittent, Flight_Leg_No FROM Airline_MDC_Data  WHERE Equation_ID='"+equation_id+"' AND aircraftno = '"+str(aircraft_no)+"' AND Flight_Phase IS NOT NULL AND DateAndTime BETWEEN '"+from_dt+"' AND '"+to_dt+"'"
    elif is_flight_phase_enabled == 1: #include current message
        sql = "SELECT Intermittent AS OccurencesOfIntermittent, Flight_Leg_No FROM Airline_MDC_Data  WHERE Equation_ID='"+equation_id+"' AND aircraftno = '"+str(aircraft_no)+"' AND Intermittent IS NOT NULL AND DateAndTime BETWEEN '"+from_dt+"' AND '"+to_dt+"'"
    print(sql)
    try:
        conn = pyodbc.connect(driver=db_driver, host=hostname, database=db_name,
                              user=db_username, password=db_password)
        chart5_sql_df = pd.read_sql(sql, conn)
        # MDCdataDF.columns = column_names
        conn.close()
        return chart5_sql_df
    except pyodbc.Error as err:
        print("Couldn't connect to Server")
        print("Error message:- " + str(err))

@app.post("/api/chart_five/{aircraft_no}/{equation_id}/{is_flight_phase_enabled}/{fromDate}/{toDate}")
async def get_CharFiveData(aircraft_no:int, equation_id:str, is_flight_phase_enabled:int, fromDate: str , toDate: str):
    return chart5Report(aircraft_no, equation_id, is_flight_phase_enabled, fromDate, toDate)
    # chart5_sql_df = connect_database_for_chart5(aircraft_no, equation_id, is_flight_phase_enabled, fromDate, toDate)
    # chart5_sql_df.replace({'>':0}, inplace=True) #To replace the error value '>' with 0 in Intermittent Column.
    # chart5_sql_df_json = chart5_sql_df.to_json(orient='records')
    # return chart5_sql_df_json



"""
#Landing Page Chart: Scatter Plot
import datetime


def connect_database_MDCData_Filtered(date_entered):
    date_entered_new = datetime.datetime.strptime(date_entered, '%m-%d-%Y')
    backdate = date_entered_new - datetime.timedelta(days=1)
    leading_date = date_entered_new - datetime.timedelta(days=8)
    leading_date_formatted = datetime.datetime.strftime(leading_date, '%m-%d-%Y')
    backdate_formatted = datetime.datetime.strftime(backdate, '%m-%d-%Y')
    print(leading_date_formatted)
    print(backdate_formatted)
    sql = "SELECT * FROM Airline_MDC_Data WHERE DateAndTime BETWEEN '" + backdate_formatted + "' AND '" + leading_date_formatted + "'"
    try:
        conn = pyodbc.connect(driver=db_driver, host=hostname, database=db_name,
                              user=db_username, password=db_password)
        # add column names from csv file into dataframe
        MDCDataFiltered = pd.read_sql(sql, conn)
        conn.close()
        return MDCDataFiltered
    except pyodbc.Error as err:
        print("Couldn't connect to Server")
        print("Error message:- " + err)

def connect_database_PMData_Filtered(date_entered):
    #global MXIDataDF
    date_entered_new = datetime.datetime.strptime(date_entered, '%m-%d-%Y')
    backdate = date_entered_new - datetime.timedelta(days=30)
    leading_date = date_entered_new - datetime.timedelta(days=8)
    leading_date_formatted = datetime.datetime.strftime(leading_date, '%m-%d-%Y')
    backdate_formatted = datetime.datetime.strftime(backdate, '%m-%d-%Y')
    print(leading_date_formatted)
    print(backdate_formatted)
    sql = "SELECT * FROM SKW_PM_NOV_2020_post_MHIRJ_filter WHERE SNAG_DATE BETWEEN '" + backdate_formatted + "' AND '" + leading_date_formatted + "'"
    try:
        conn = pyodbc.connect(driver=db_driver, host=hostname, database=db_name,
                              user=db_username, password=db_password)
        # add column names from csv file into dataframe
        PMDataFiltered = pd.read_sql(sql, conn)
        conn.close()
        return PMDataFiltered
    except pyodbc.Error as err:
        print("Couldn't connect to Server")
        print("Error message:- " + err)

def connect_database_for_MDC_ScatterPlot(leading_date):
    leading_date_n = datetime.datetime.strptime(leading_date, '%m-%d-%Y')
    backdate = leading_date_n - datetime.timedelta(days = 7)
    leading_date_formatted = datetime.datetime.strftime(leading_date_n, '%m-%d-%Y')
    backdate_formatted = datetime.datetime.strftime(backdate, '%m-%d-%Y')
    print(leading_date_formatted)
    print(backdate_formatted)
    sql = "SELECT COUNT(Airline_MDC_Data.MDC_Message) as '# of MDC Messages' FROM Airline_MDC_Data WHERE DateAndTime BETWEEN '"+str(backdate_formatted)+"' AND '"+str(leading_date_formatted)+"' GROUP BY aircraftno"
    try:
        conn = pyodbc.connect(driver=db_driver, host=hostname, database=db_name,
                              user=db_username, password=db_password)
        MDC_ScatterPlot_sql_df = pd.read_sql(sql, conn)
        conn.close()
        return MDC_ScatterPlot_sql_df
    except pyodbc.Error as err:
        print("Couldn't connect to Server")
        print("Error message:- " + str(err))

def connect_database_for_MDC_ScatterPlot_static():
    
    sql = "SELECT COUNT(Airline_MDC_Data.MDC_Message) as '# of MDC Messages' FROM Airline_MDC_Data WHERE DateAndTime BETWEEN '11-5-2020' AND '11-12-2020' GROUP BY aircraftno"
    try:
        conn = pyodbc.connect(driver=db_driver, host=hostname, database=db_name,
                              user=db_username, password=db_password)
        MDC_ScatterPlot_sql_df = pd.read_sql(sql, conn)
        conn.close()
        return MDC_ScatterPlot_sql_df
    except pyodbc.Error as err:
        print("Couldn't connect to Server")
        print("Error message:- " + str(err))


def connect_database_for_PM_ScatterPlot(leading_date):
    leading_date_n = datetime.datetime.strptime(leading_date, '%m-%d-%Y')
    backdate = leading_date_n - datetime.timedelta(days = 7)
    leading_date_formatted = datetime.datetime.strftime(leading_date_n, '%m-%d-%Y')
    backdate_formatted = datetime.datetime.strftime(backdate, '%m-%d-%Y')
    print(leading_date_formatted)
    print(backdate_formatted)
    sql = "SELECT COUNT(CORRECTIVE_ACTION) as '# of MX Actions' FROM SKW_PM_NOV_2020_post_MHIRJ_filter WHERE SNAG_DATE BETWEEN '"+str(backdate_formatted)+"' AND '"+str(leading_date_formatted)+"' AND AC_MODEL = 'CRJ700' GROUP BY AC_SN"
    try:
        conn = pyodbc.connect(driver=db_driver, host=hostname, database=db_name,
                              user=db_username, password=db_password)
        PM_ScatterPlot_sql_df = pd.read_sql(sql, conn)
        conn.close()
        return PM_ScatterPlot_sql_df
    except pyodbc.Error as err:
        print("Couldn't connect to Server")
        print("Error message:- " + str(err))


def connect_database_for_PM_ScatterPlot_static():
   
    sql = "SELECT COUNT(CORRECTIVE_ACTION) as '# of MX Actions' FROM SKW_PM_NOV_2020_post_MHIRJ_filter WHERE SNAG_DATE BETWEEN '11-5-2020' AND '11-12-2020' AND AC_MODEL = 'CRJ700' GROUP BY AC_SN"
    try:
        conn = pyodbc.connect(driver=db_driver, host=hostname, database=db_name,
                              user=db_username, password=db_password)
        PM_ScatterPlot_sql_df = pd.read_sql(sql, conn)
        conn.close()
        return PM_ScatterPlot_sql_df
    except pyodbc.Error as err:
        print("Couldn't connect to Server")
        print("Error message:- " + str(err))
"""

## Landing Page Chart - Scatter Plot
def connect_database_for_scatter_plot():

    sql = "EXEC Getaircraftstatsv2"

    print(sql)
    try:
        conn = pyodbc.connect(driver=db_driver, host=hostname, database=db_name,
                              user=db_username, password=db_password)
        scatter_chart_sql_df = pd.read_sql(sql, conn)
        conn.close()
        return scatter_chart_sql_df
    except pyodbc.Error as err:
        print("Couldn't connect to Server")
        print("Error message:- " + str(err))
@app.post("/api/scatter_chart_MDC_PM")
async def get_ScatterChart_MDC_PM_Data():
    scatter_chart_sql_df = connect_database_for_scatter_plot()
    scatter_chart_sql_df_json = scatter_chart_sql_df.to_json(orient='records')
    return scatter_chart_sql_df_json


def connect_database_for_scatter_plot_v2(from_date, to_date):

    sql = "EXEC Getaircraftstatsv2 '"+from_date+"', '"+to_date+"'"
    print(sql)
    try:
        conn = pyodbc.connect(driver=db_driver, host=hostname, database=db_name,
                              user=db_username, password=db_password)
        scatter_chart_sql_df = pd.read_sql(sql, conn)
        conn.close()
        return scatter_chart_sql_df
    except pyodbc.Error as err:
        print("Couldn't connect to Server")
        print("Error message:- " + str(err))

#For reference -> http://localhost:8000/scatter_chart_MDC_PM_Data/2020-11-12
@app.post("/api/scatter_chart_MDC_PM/{from_date}/{to_date}")
async def get_ScatterChart_MDC_PM_Data(from_date:str, to_date:str):
    scatter_chart_sql_df = connect_database_for_scatter_plot_v2(from_date, to_date)
    scatter_chart_sql_df_json = scatter_chart_sql_df.to_json(orient='records')
    return scatter_chart_sql_df_json


### Landing Chart B

def connect_db_MDCdata_chartb_static():
    end_date = datetime.datetime.utcnow()
    start_date = end_date - datetime.timedelta(days=10)

    end_date = datetime.datetime.strftime(end_date, '%m-%d-%Y')
    start_date = datetime.datetime.strftime(start_date, '%m-%d-%Y')
    sql = "SELECT * FROM Airline_MDC_Data WHERE DateAndTime BETWEEN '"+start_date+"' AND '"+end_date+"'"
    column_names = ["Aircraft", "Tail", "Flight Leg No",
                    "ATA Main", "ATA Sub", "ATA", "ATA Description", "LRU",
                    "DateAndTime", "MDC Message", "Status", "Flight Phase", "Type",
                    "Intermittent", "Equation ID", "Source", "Diagnostic Data",
                    "Data Used to Determine Msg", "ID", "Flight", "airline_id", "aircraftno"]
    print(sql)
    try:
        conn = pyodbc.connect(driver=db_driver, host=hostname, database=db_name,
                              user=db_username, password=db_password)
        MDCdataDF_chartb = pd.read_sql(sql, conn)
        MDCdataDF_chartb.columns = column_names
        conn.close()
        return MDCdataDF_chartb
    except pyodbc.Error as err:
        print("Couldn't connect to Server")
        print("Error message:- " + str(err))

# for reference -> http://localhost:8000/Landing_Chart_B
@app.post("/api/Landing_Chart_B")
async def get_Chart_B():
    MDCdataDF_chartb = connect_db_MDCdata_chartb_static()
    try:
        Topvalues2 = 10
        MDCdataDF = connect_db_MDCdata_chartb(from_dt, to_dt)
        AircraftTailPairDF = MDCdataDF[["Aircraft", "Tail"]].drop_duplicates(ignore_index= True) # unique pairs of AC SN and Tail# for use in analysis
        AircraftTailPairDF.columns = ["AC SN","Tail"] # re naming the columns to match History/Daily analysis output
        chartADF = pd.merge(left = MDCdataDF[["Aircraft","ATA Main", "Equation ID"]], right = AircraftTailPairDF, left_on="Aircraft", right_on="AC SN")
        chartADF["Aircraft"] = chartADF["Aircraft"] + " / " + chartADF["Tail"]
        chartADF.drop(labels = ["AC SN", "Tail"], axis = 1, inplace = True)
        MessageCountbyAircraftATA = chartADF.groupby(["Aircraft","ATA Main"]).count()
        # https://towardsdatascience.com/stacked-bar-charts-with-pythons-matplotlib-f4020e4eb4a7
        # https://stackoverflow.com/questions/44309507/stacked-bar-plot-using-matplotlib
        # transpose the indexes. where the ATA label becomes the column and the aircraft is row. counts are middle
        TransposedMessageCountbyAircraftATA = MessageCountbyAircraftATA["Equation ID"].unstack()

        # fill Null values with 0
        TransposedMessageCountbyAircraftATA.fillna(value= 0, inplace= True)

        # sum all the counts by row, plus create a new column called sum
        TransposedMessageCountbyAircraftATA["Sum"] = TransposedMessageCountbyAircraftATA.sum(axis=1)

        # sort the dataframe by the values of sum, and from the topvalues2 the user chooses
        TransposedMessageCountbyAircraftATA = TransposedMessageCountbyAircraftATA.sort_values("Sum",ascending=False).tail(Topvalues2)

        # create a final dataframe for plotting without the new column created before
        TransposedMessageCountbyAircraftATAfinalPLOT = TransposedMessageCountbyAircraftATA.drop(["Sum"], axis=1)

        totals = TransposedMessageCountbyAircraftATA["Sum"]
        print("total in landing chart B is : ",totals)
        chart_b_df_json = TransposedMessageCountbyAircraftATAfinalPLOT.to_json(orient='index')
        return chart_b_df_json
    except Exception as es :
 	    print(es)
    


def connect_db_MDCdata_chartb(from_dt, to_dt):
    sql = "SELECT * FROM Airline_MDC_Data WHERE DateAndTime BETWEEN '" + from_dt + "' AND '" + to_dt + "'"
    column_names = ["Aircraft", "Tail", "Flight Leg No",
                    "ATA Main", "ATA Sub", "ATA", "ATA Description", "LRU",
                    "DateAndTime", "MDC Message", "Status", "Flight Phase", "Type",
                    "Intermittent", "Equation ID", "Source", "Diagnostic Data",
                    "Data Used to Determine Msg", "ID", "Flight", "airline_id", "aircraftno"]
    print(sql)
    try:
        conn = pyodbc.connect(driver=db_driver, host=hostname, database=db_name,
                              user=db_username, password=db_password)
        MDCdataDF_chartb = pd.read_sql(sql, conn)
        MDCdataDF_chartb.columns = column_names
        conn.close()
        return MDCdataDF_chartb
    except pyodbc.Error as err:
        print("Couldn't connect to Server")
        print("Error message:- " + str(err))

#with ata chartb
def connect_db_MDCdata_chartb_ata(ata,from_dt, to_dt):
    sql = "SELECT * FROM Airline_MDC_Data WHERE ATA_Main IN " + str(ata) +" and  DateAndTime BETWEEN '" + from_dt + " 00:00:00 ' AND '" + to_dt + " 23:59:59 '"
    column_names = ["Aircraft", "Tail", "Flight Leg No",
                    "ATA Main", "ATA Sub", "ATA", "ATA Description", "LRU",
                    "DateAndTime", "MDC Message", "Status", "Flight Phase", "Type",
                    "Intermittent", "Equation ID", "Source", "Diagnostic Data",
                    "Data Used to Determine Msg", "ID", "Flight", "airline_id", "aircraftno"]
    print(sql)
    try:
        conn = pyodbc.connect(driver=db_driver, host=hostname, database=db_name,
                              user=db_username, password=db_password)
        MDCdataDF_chartb = pd.read_sql(sql, conn)
        MDCdataDF_chartb.columns = column_names
        conn.close()
        return MDCdataDF_chartb
    except pyodbc.Error as err:
        print("Couldn't connect to Server")
        print("Error message:- " + str(err))

# for reference -> http://localhost:8000/Landing_Chart_B/15/11-11-2020/11-17-2020
@app.post("/api/Landing_Chart_B/{ata}/{top_n}/{from_dt}/{to_dt}")
async def get_Chart_B(ata:str,top_n: int,from_dt: str, to_dt: str):
    try:
        Topvalues2 = top_n
        if Topvalues2>50:
            Topvalues2=50
        
        MDCdataDF = connect_db_MDCdata_chartb_ata(ata,from_dt, to_dt)
        AircraftTailPairDF = MDCdataDF[["Aircraft", "Tail"]].drop_duplicates(ignore_index= True) # unique pairs of AC SN and Tail# for use in analysis
        AircraftTailPairDF.columns = ["AC SN","Tail"] # re naming the columns to match History/Daily analysis output
        chartADF = pd.merge(left = MDCdataDF[["Aircraft","ATA Main", "Equation ID"]], right = AircraftTailPairDF, left_on="Aircraft", right_on="AC SN")
        chartADF["Aircraft"] = chartADF["Aircraft"] + " / " + chartADF["Tail"]
        chartADF.drop(labels = ["AC SN", "Tail"], axis = 1, inplace = True)
        MessageCountbyAircraftATA = chartADF.groupby(["Aircraft","ATA Main"]).count()
        # https://towardsdatascience.com/stacked-bar-charts-with-pythons-matplotlib-f4020e4eb4a7
        # https://stackoverflow.com/questions/44309507/stacked-bar-plot-using-matplotlib
        # transpose the indexes. where the ATA label becomes the column and the aircraft is row. counts are middle
        TransposedMessageCountbyAircraftATA = MessageCountbyAircraftATA["Equation ID"].unstack()

        # fill Null values with 0
        TransposedMessageCountbyAircraftATA.fillna(value= 0, inplace= True)

        # sum all the counts by row, plus create a new column called sum
        TransposedMessageCountbyAircraftATA["Sum"] = TransposedMessageCountbyAircraftATA.sum(axis=1)

        # sort the dataframe by the values of sum, and from the topvalues2 the user chooses
        TransposedMessageCountbyAircraftATA = TransposedMessageCountbyAircraftATA.sort_values("Sum").tail(Topvalues2)
        TransposedMessageCountbyAircraftATA = TransposedMessageCountbyAircraftATA.sort_values("Sum", ascending=False)

        # create a final dataframe for plotting without the new column created before
        TransposedMessageCountbyAircraftATAfinalPLOT = TransposedMessageCountbyAircraftATA.drop(["Sum"], axis=1)
        print('TransposedMessageCountbyAircraftATAfinalPLOT colums : ',TransposedMessageCountbyAircraftATAfinalPLOT.columns)
        #totals = TransposedMessageCountbyAircraftATA["Sum"]
        print("total in landing chart B is : ",TransposedMessageCountbyAircraftATAfinalPLOT)
        # TransposedMessageCountbyAircraftATAfinalPLOT = TransposedMessageCountbyAircraftATAfinalPLOT.sort_values(by='ATA Main',ascending=False)
        chart_b_df_json = TransposedMessageCountbyAircraftATAfinalPLOT.to_json(orient='index')
	    #return chart_b_df_json
        # #image settings
        # ax8 = TransposedMessageCountbyAircraftATAfinalPLOT.plot(kind='barh', stacked=True, figsize=(16, 9))
        # ax8.set_ylabel('Aircraft Serial Number')
        # ax8.set_title('Magnitude of messages in data')
        # ax8.grid(b= True, which= "both", axis= "x", alpha= 0.3)
        # rects8 = ax8.containers[-1] 


        # # here to add column labeling
        # for i, total in enumerate(totals):
        #     ax8.text(totals[i], rects8[i].get_y() +0.15 , round(total), ha='left')
            
        # plt.show()
        return chart_b_df_json
    except Exception as es :
 	    print(es)




"""
@app.post("/api/Landing_Chart_B/{top_n}/{from_dt}/{to_dt}")
# async def get_Chart_B(top_n: int,from_dt: str, to_dt: str):
	try:
	    MDCdataDF_chartb = connect_db_MDCdata_chartb(from_dt, to_dt)
	    Topvalues2 = top_n
	    # groups the data by Aircraft and Main ATA, produces a count of values in each ata by counting entries in Equation ID
	    MessageCountbyAircraftATA = MDCdataDF_chartb[["aircraftno", "ATA Main", "Equation ID"]].groupby(
		["aircraftno", "ATA Main"]).count()
	    # transpose the indexes. where the ATA label becomes the column and the aircraft is row. counts are middle
	    TransposedMessageCountbyAircraftATA = MessageCountbyAircraftATA["Equation ID"].unstack()
	    # fill Null values with 0
	    TransposedMessageCountbyAircraftATA.fillna(value=0, inplace=True)
	    # sum all the counts by row, plus create a new column called sum
	    TransposedMessageCountbyAircraftATA["Sum"] = TransposedMessageCountbyAircraftATA.sum(axis=1)
	    # sort the dataframe by the values of sum, and from the topvalues2 the user chooses
	    TransposedMessageCountbyAircraftATA = TransposedMessageCountbyAircraftATA.sort_values("Sum").tail(Topvalues2)
	    # create a final dataframe for plotting without the new column created before
	    TransposedMessageCountbyAircraftATAfinalPLOT = TransposedMessageCountbyAircraftATA.drop(["Sum"], axis=1)
	    print(TransposedMessageCountbyAircraftATAfinalPLOT)
	    chart_b_df_json = TransposedMessageCountbyAircraftATAfinalPLOT.to_json(orient='index')
	    return chart_b_df_json
	except Exception as es :
 		print(es)"""


"""
## Landing Page Chart - StackedBar Chart
def connect_database_for_stacked_plot():

    sql = "EXEC Getlandingpagechart"

    print(sql)
    try:
        conn = pyodbc.connect(driver=db_driver, host=hostname, database=db_name,
                              user=db_username, password=db_password)
        stacked_chart_sql_df = pd.read_sql(sql, conn)
        conn.close()
        return stacked_chart_sql_df
    except pyodbc.Error as err:
        print("Couldn't connect to Server")
        print("Error message:- " + str(err))
@app.post("api/stackedbar_chart_MDCmessages")
async def get_stackedbar_Chart_MDC_PM_Data():
    stacked_chart_sql_df = connect_database_for_stacked_plot()
    stacked_chart_sql_df_json = stacked_chart_sql_df.to_json(orient='records')
    return stacked_chart_sql_df_json

def connect_database_for_stacked_plot_v2(start_date, end_date, top_value):

    sql = "EXEC Getlandingpagechart2 '"+start_date+"','"+end_date+"',"+str(top_value)+""
    print(sql)
    try:
        conn = pyodbc.connect(driver=db_driver, host=hostname, database=db_name,
                              user=db_username, password=db_password)
        stackedbarv2_chart_sql_df = pd.read_sql(sql, conn)
        conn.close()
        return stackedbarv2_chart_sql_df
    except pyodbc.Error as err:
        print("Couldn't connect to Server")
        print("Error message:- " + str(err))

#For reference -> http://localhost:8000/stacked_chart_MDC_PM_Data/2020-11-12/2020-11-13/15
@app.post("/stacked_chart_MDC_PM/{start_date}/{end_date}/{top_value}")
async def get_Stacked_Chart_MDC_PM_Data(start_date:str,end_date:str,top_value:int):
    stackedbarv2_sql_df = connect_database_for_stacked_plot_v2(start_date,end_date,top_value)
    stackedbarv2_chart_sql_df_json = stackedbarv2_sql_df.to_json(orient='records')
    return stackedbarv2_chart_sql_df_json
"""


#### Corelation Stored Proc Call
def connect_database_for_corelation(from_dt, to_dt, equation_id, ata):
    #equation_id = str(tuple(equation_id.replace(")", "").replace("(", "").replace("'", "").split(",")))
    #if "ALL" not in ata:
    #    ata = str(tuple(ata.replace(")", "").replace("(", "").replace("'", "").split(",")))

    #equation_id = str(tuple(equation_id.replace(")","").replace("(","").replace("'","").split(",")))
    #ata = str(tuple(ata.replace(")","").replace("(","").replace("'","").split(",")))
    sql =""

    sql += "select distinct [MaintTransID],[EQ_ID], [DateAndTime],[Failure_Flag],[MRB],[SquawkSource],Substring(ATA, 1,2) ATA,[Discrepancy],[CorrectiveAction] from [dbo].[MDC_PM_Correlated] where CONVERT(date,DateAndTime) between '" + from_dt + "'  AND '" + to_dt + "'"
    #sql += "select distinct MaintTransID p_ID, Aircraft_tail_No, aircraftno, EQ_ID, EQ_DESCRIPTION, LRU,CAS, MDC_MESSAGE, Substring(ATA, 1,2) ATA, Discrepancy, CorrectiveAction, DateAndTime, Failure_Flag, SquawkSource from [dbo].[MDC_PM_Correlated] where Status = 3 AND CONVERT(date,DateAndTime) between '" + from_dt + "'  AND '" + to_dt + "'"
    print("len of eq_id",equation_id)
    if equation_id!="":
        if ',' in equation_id:
            equation_id = str(tuple(equation_id.replace(")", "").replace("(", "").split(",")))
            equation_id = equation_id.replace(equation_id[len(equation_id)-2], '')
            sql += "  AND EQ_ID IN " + equation_id
        else : 
            #equation_id = str(tuple(equation_id.replace(")", "").replace("(", "").replace("'", "").split(",")))
            # if len(equation_id) >= 14:
            #equation_id = equation_id.replace(equation_id[len(equation_id)-2], '')
            sql += "  AND EQ_ID = " + equation_id
    if "ALL" not in ata :
        if ata!="":
            sql += "  AND Substring(ATA, 1,2) IN " + ata
    print(sql)
    try:
        conn = pyodbc.connect(driver=db_driver, host=hostname,
                              database=db_name,
                              user=db_username, password=db_password)
        report_eqId_sql_df = pd.read_sql(sql, conn)
        conn.close()
        return report_eqId_sql_df
    except pyodbc.Error as err:
        print("Couldn't connect to Server")
        print("Error message:- " + str(err))

@app.post("/api/corelation_new/{fromDate}/{toDate}/{equation_id}/{tail_no}")
async def get_NewCorelation(fromDate: str, toDate: str, equation_id:str, tail_no:str):
    corelation_df = connect_database_for_corelation_new(fromDate, toDate, equation_id, tail_no)
    corelation_df_json = corelation_df.to_json(orient='records')
    return corelation_df_json


def connect_database_for_corelation_new(from_dt, to_dt, equation_id, tail_no):
    sql =""

    sql += "SELECT DISTINCT [MaintTransID],[DateAndTime],[Failure_Flag],[MRB],[SquawkSource],[Discrepancy],[CorrectiveAction] FROM [dbo].[MDC_PM_Correlated] WHERE Aircraft_tail_No = '" + tail_no + "' AND EQ_ID = '"+equation_id+"' AND CONVERT(date,DateAndTime) BETWEEN '" + from_dt + "'  AND '" + to_dt + "'"
    print(sql)
    try:
        conn = pyodbc.connect(driver=db_driver, host=hostname,
                              database=db_name,
                              user=db_username, password=db_password)
        report_eqId_sql_df = pd.read_sql(sql, conn)
        conn.close()
        return report_eqId_sql_df
    except pyodbc.Error as err:
        print("Couldn't connect to Server")
        print("Error message:- " + str(err))

# for reference -> http://localhost:8000/corelation/11-11-2020/11-12-2020/B1-008003/27
@app.post("/api/corelation/{fromDate}/{toDate}")
async def get_CorelationData(fromDate: str, toDate: str, equation_id:Optional[str]="", ata:Optional[str]=""):
    corelation_df = connect_database_for_corelation(fromDate, toDate, equation_id, ata)
    corelation_df_json = corelation_df.to_json(orient='records')
    return corelation_df_json


def connect_database_for_corelation_pid(p_id):
    
    sql = """SELECT 
	[Aircraft_tail_No],
	[EQ_ID],
	[aircraftno],
	[ATA_Description],
	[LRU],
	[CAS],
	[MDC_MESSAGE],
	[EQ_DESCRIPTION],
	[ATA_Main],
	[ATA_Sub]
    FROM [dbo].[MDC_PM_Correlated] 
    WHERE [MaintTransID] = %s
    """ %(p_id)

    print(sql)

    try:
        conn = pyodbc.connect(driver=db_driver, host=hostname, database=db_name,
                              user=db_username, password=db_password)
        corelation_df = pd.read_sql(sql, conn)
        print('query successful')
        conn.close()
        return corelation_df
    except pyodbc.Error as err:
        print("Couldn't connect to Server")
        print("Error message:- " + str(err))

@app.post("/api/corelation/{p_id}")
async def get_CorelationDataPID(p_id: str):
    corelation_df = connect_database_for_corelation_pid(p_id)
    print('corelation func :',corelation_df)
    corelation_df_json = corelation_df.to_json(orient='records')
    return corelation_df_json

def connect_database_for_corelation_pid2(p_id):
    sql = """SELECT [mdc_ID], [EQ_ID], [aircraftno], [ATA_Description], [LRU], [DateAndTime], [MDC_Date], 
	[MDC_MESSAGE], [EQ_DESCRIPTION], [CAS], [LRU_CODE], [LRU_NAME], [FAULT_LOGGED], [MDC_ATA], 
	[mdc_ata_main], [mdc_ata_sub], [Status], [mdc_type]
    FROM [dbo].[sample_corelation]
    WHERE p_id = (%s)
    ORDER BY MDC_Date""" % (p_id)

    print(sql)

    try:
        conn = pyodbc.connect(driver=db_driver, host=hostname,
                              database=db_name,
                              user=db_username, password=db_password)
        corelation_df = pd.read_sql(sql, conn)
        print('query successful')
        conn.close()
        return corelation_df
    except pyodbc.Error as err:
        print("Couldn't connect to Server")
        print("Error message:- " + str(err))


@app.post("/api/corelation/{p_id}")
async def get_CorelationDataPID(p_id: str):
    corelation_df = connect_database_for_corelation_pid(p_id)
    print('corelation func :', corelation_df)
    corelation_df_json = corelation_df.to_json(orient='records')
    return corelation_df_json

#Correlation_process_status
#31st August
def connect_correlation_process_status():
    sql = "SELECT DISTINCT Correlation_Process_Status.ID,Correlation_Process_Status.Process,Correlation_Process_Status.Status,Correlation_Process_Status.Status_Message,Correlation_Process_Status.Date FROM Correlation_Process_Status ORDER BY Correlation_Process_Status.Date DESC"

    try:
        conn = pyodbc.connect(driver=db_driver, host=hostname, database=db_name,
                              user=db_username, password=db_password)
        correlation_process_status = pd.read_sql(sql, conn)
        #MDCdataDF.columns = column_names
        return correlation_process_status
    except pyodbc.Error as err:
        print("Couldn't connect to Server")
        print("Error message:- " + str(err))

@app.post("/api/corelation_process_status")
async def get_correlation_process_status():
    correlation_process_status = connect_correlation_process_status()
    correlation_process_status_json = correlation_process_status.to_json(orient='records')
    return correlation_process_status_json

def connect_database_for_eqId(all):
    sql = "SELECT DISTINCT Airline_MDC_Data.Equation_ID FROM Airline_MDC_Data"

    try:
        conn = pyodbc.connect(driver=db_driver, host=hostname, database=db_name,
                              user=db_username, password=db_password)
        report_eqId_sql_df = pd.read_sql(sql, conn)
        #MDCdataDF.columns = column_names
        return report_eqId_sql_df
    except pyodbc.Error as err:
        print("Couldn't connect to Server")
        print("Error message:- " + str(err))



@app.post("/api/GenerateReport/equation_id/{all}")
async def get_eqIData(all:str):
    report_eqId_sql_df = connect_database_for_eqId(all)
    report_eqId_sql_df_json = report_eqId_sql_df.to_json(orient='records')
    return report_eqId_sql_df_json


def connect_database_for_ata_main(all):
    sql = "SELECT DISTINCT Airline_MDC_Data.ATA_Main FROM Airline_MDC_Data"

    try:
        conn = pyodbc.connect(driver=db_driver, host=hostname, database=db_name,
                              user=db_username, password=db_password)
        report_ata_main_sql_df = pd.read_sql(sql, conn)
        #MDCdataDF.columns = column_names
        return report_ata_main_sql_df
    except pyodbc.Error as err:
        print("Couldn't connect to Server")
        print("Error message:- " + str(err))



@app.post("/api/GenerateReport/ata_main/{all}")
async def get_eqIData(all:str):
    report_ata_main_sql_df = connect_database_for_ata_main(all)
    report_ata_main_sql_df_json = report_ata_main_sql_df.to_json(orient='records')
    return report_ata_main_sql_df_json

@app.post("/api/uploadfile_airline_mdc_raw_data/")
async def create_upload_file(file: UploadFile = File(...)):
    result = insertData(file)
    return {"result": result}      
    
@app.post("/api/uploadfile_input_message_data/")
async def create_upload_file1(file: UploadFile = File(...)):
    result = insertData_MDCMessageInputs(file)
    return {"result": result} 

# update input message data    
def connect_database_for_update(Equation_ID,EICAS,Priority_,MHIRJ_ISE_inputs,MHIRJ_ISE_Recommended_Action,Additional_Comments,MEL_or_No_Dispatch):
   # try:
         
        conn = pyodbc.connect(driver=db_driver, host=hostname, database=db_name,
                              user=db_username, password=db_password)
        cursor = conn.cursor()  
        sql =" UPDATE  MDCMessagesInputs_CSV_UPLOAD SET "

        sqlConditions = ''
        numberOfConditions = 0
        if  EICAS.strip() :
            numberOfConditions += 1
            sql +=  " [EICAS]=  '" + EICAS + "' "
                               
        if  Priority_.strip() :
            numberOfConditions += 1
            sql += " , " if numberOfConditions > 1 else sql
            sql +=  " [Priority_] = '" + Priority_ + "'"

        if  MHIRJ_ISE_inputs.strip() :
            numberOfConditions += 1
            sql += " , " if numberOfConditions > 1 else sql
            sql +=  "[MHIRJ_ISE_inputs] = '" + MHIRJ_ISE_inputs + "'"

        if  MHIRJ_ISE_Recommended_Action.strip() :
            numberOfConditions += 1
            sql += " , " if numberOfConditions > 1 else sql
            sql +=  "[MHIRJ_ISE_Recommended_Action] = '" + MHIRJ_ISE_Recommended_Action + "'"

        if  Additional_Comments.strip() :
            numberOfConditions += 1
            sql += " , " if numberOfConditions > 1 else sql
            sql +=  "[Additional_Comments] = '" + Additional_Comments + "'"     


        if  MEL_or_No_Dispatch.strip() :
            numberOfConditions += 1
            sql += " , " if numberOfConditions > 1 else sql
            sql +=  "[MEL_or_No_Dispatch] = '" + MEL_or_No_Dispatch + "'"                
 
        sql += "  WHERE [Equation_ID] = '" + Equation_ID + "'"
 
        # print("------ATA ----")
        # print(ATA)
        print("---print sql builder----")
        print(sql)
 
        cursor.execute(sql)
               
 
        # sql+= cursor.execute(" UPDATE  MDCMessagesInputs_CSV_UPLOAD SET [LRU]= ? , [ATA]=? WHERE [Equation_ID]=?",
        #           LRU,
        #           ATA,
        #           equation_id
        #         ) 
       
 
 
        # update_sql_df = pd.read_sql(sql, conn)
        # MDCdataDF.columns = column_names
        conn.commit()
        conn.close()
        # return update_sql_df
        return "Successfully UPDATE into MDCMessagesInputs"
 
    # except pyodbc.Error as err:
    #     print("Couldn't connect to Server")
    #     print("Error message:- " + str(err))
 
@app.post("/api/update_input_message_data/{Equation_ID}/{EICAS}/{Priority_}/{MHIRJ_ISE_inputs}/{MHIRJ_ISE_Recommended_Action}/{Additional_Comments}/{MEL_or_No_Dispatch}")
async def update_data(Equation_ID:str, EICAS:str,Priority_:str, MHIRJ_ISE_inputs:str, MHIRJ_ISE_Recommended_Action:str ,Additional_Comments:str,MEL_or_No_Dispatch:str):
    update_data = connect_database_for_update(Equation_ID,EICAS,Priority_,MHIRJ_ISE_inputs,MHIRJ_ISE_Recommended_Action,Additional_Comments,MEL_or_No_Dispatch)
    return update_data


# delete from MDC messege input    

@app.post("/api/delete/")
async def delete():
    delete_data = connect_database_for_delete()
    return delete_data    

def connect_database_for_delete():
   
         
        conn = pyodbc.connect(driver=db_driver, host=hostname, database=db_name,
                              user=db_username, password=db_password)
        cursor = conn.cursor()  
        sql =" DELETE FROM MDCMessagesInputs_CSV_UPLOAD"
        print(sql)
 
        cursor.execute(sql)
        conn.commit()
        conn.close()
        # return update_sql_df
        return "Delete from MDCMessagesInputs"  

# Select all data from MDC messege input   
def connect_database_for_mdcMessageData():
    sql = "SELECT * From MDCMessagesInputs_CSV_UPLOAD"

    try:
        conn = pyodbc.connect(driver=db_driver, host=hostname, database=db_name,
                              user=db_username, password=db_password)
        mdcMessage_df = pd.read_sql(sql, conn)
        #MDCdataDF.columns = column_names
        return mdcMessage_df
    except pyodbc.Error as err:
        print("Couldn't connect to Server")
        print("Error message:- " + str(err))



@app.post("/api/MDC_message_data")
async def get_mdcMessageData():
    mdcMessage_df = connect_database_for_mdcMessageData()
    mdcMessage_df_json =  mdcMessage_df.to_json(orient='records')
    return  mdcMessage_df_json


   

#upload top message data     
@app.post("/api/uploadfile_top_message_data/")
async def create_upload_file2(file: UploadFile = File(...)):
    result = insertData_TopMessageSheet(file)
    return {"result": result}   


## Delta Report
True_list = []
False_list = []
def create_delta_lists(prev_history, curr_history):
    '''create True and False lists for the Delta report'''
    # making tuples of the combos of AC SN and B1-eqn
    comb_prev = list(zip(prev_history["AC SN"], prev_history["B1-Equation"]))
    comb_curr = list(zip(curr_history["AC SN"], curr_history["B1-Equation"]))

    # create a list for flags still on going (true_list) and new flags (false_list)
    True_list = []
    False_list = []
    for i in range(len(comb_curr)):
        if comb_curr[i] in comb_prev:
            True_list.append(comb_curr[i])
        elif comb_curr[i] not in comb_prev:
            False_list.append(comb_curr[i])

       
        
    return True_list, False_list

    


def highlight_delta(table_highlight, delta_list, Jam_list=listofJamMessages):
    '''highlights delta of two history reports'''
    # check what in the new report exists in the prev. which of tuple(AC SN, B1) exists in the true list
    #is_color = (table_highlight["AC SN"], table_highlight["B1-Equation"]) in delta_list
    #is_color = table_highlight.index.values in delta_list
    #print((table_highlight["AC SN"], table_highlight["B1-Equation"]) in delta_list)

    #print(table_highlight.iloc[0]["AC SN"], table_highlight.iloc[0]["B1-Equation"])
    #print("table_highlight[0]: ",table_highlight[0])
    #print("index of th: ", table_highlight.index)
    #print("tuple of index: ", list(zip(table_highlight['AC SN'], table_highlight['B1-Equation'])))
    # print("Delta_list: ", delta_list)
    # print(":: Table highlight ::")
    # print((table_highlight))

    print("index of th 2: ", table_highlight.index.values)
    for each_tuple in table_highlight.index.values:
        if each_tuple in delta_list:
            is_color = True

           
            # already existed in prev report and is not jam
            if delta_list == True_list and table_highlight["B1-Equation"] not in Jam_list:
                # light orange: 'background-color: #fde9d9'
                return [ table_highlight['is_light_orange'] == True if is_color else '' for v in table_highlight]
            # didnt exist in prev report and is not jam
            elif delta_list == False_list and table_highlight["B1-Equation"] not in Jam_list:
                # dark orange: 'background-color: #fabf8f'
                return [ table_highlight['is_dark_orange'] == True if is_color else '' for v in table_highlight]
            # already existed in prev report and is jam
            elif delta_list == True_list and table_highlight["B1-Equation"] in Jam_list:
                # light red:  'background-color: #f08080'
                return [ table_highlight['is_light_red'] == True if is_color else '' for v in table_highlight]
            # didnt exist in prev report and is jam
            elif delta_list == False_list and table_highlight["B1-Equation"] in Jam_list:
                # dark red: 'background-color: #ff342e'
                return [ table_highlight['is_dark_red'] == True if is_color else '' for v in table_highlight]




@app.post(
    "/api/GenerateReport/{analysisType}/{occurences}/{legs}/{intermittent}/{consecutiveDays}/{ata}/{exclude_EqID}/{airline_operator}/{include_current_message}/{flag}/{prev_fromDate}/{prev_toDate}/{curr_fromDate}/{curr_toDate}")
async def generateDeltaReport(analysisType: str, occurences: int, legs: int, intermittent: int, consecutiveDays: int,
                              ata: str, exclude_EqID: str, airline_operator: str, include_current_message: int,flag,
                              prev_fromDate: str, prev_toDate: str, curr_fromDate: str, curr_toDate: str):
    
    flag=0
    ## Previous dates computation
    if not (prev_fromDate is None and prev_toDate is None):
        MDCdataDF = connect_database_MDCdata(ata, exclude_EqID, airline_operator, include_current_message,
                                             prev_fromDate, prev_toDate)

       

        # Date formatting
        MDCdataDF["DateAndTime"] = pd.to_datetime(MDCdataDF["DateAndTime"])
        # print(MDCdataDF["DateAndTime"])
        MDCdataDF["Flight Leg No"].fillna(value=0.0,
                                          inplace=True)  # Null values preprocessing - if 0 = Currentflightphase
        # print(MDCdataDF["Flight Leg No"])
        MDCdataDF["Flight Phase"].fillna(False, inplace=True)  # NuCell values preprocessing for currentflightphase
        MDCdataDF["Intermittent"].fillna(value=-1, inplace=True)  # Null values preprocessing for currentflightphase
        MDCdataDF["Intermittent"].replace(to_replace=">", value="9",
                                          inplace=True)  # > represents greater than 8 Intermittent values
        MDCdataDF["Intermittent"] = MDCdataDF["Intermittent"].astype(int)  # cast type to int

        MDCdataDF["Aircraft"] = MDCdataDF["Aircraft"].str.replace('AC', '')
        MDCdataDF.fillna(value=" ", inplace=True)  # replacing all REMAINING null values to a blank string
        MDCdataDF.sort_values(by="DateAndTime", ascending=False, inplace=True, ignore_index=True)

        AircraftTailPairDF = MDCdataDF[["Aircraft", "Tail#"]].drop_duplicates(
            ignore_index=True)  # unique pairs of AC SN and Tail# for use in analysis
        AircraftTailPairDF.columns = ["AC SN", "Tail#"]  # re naming the columns to match History/Daily analysis output

        DatesinData = MDCdataDF["DateAndTime"].dt.date.unique()  # these are the dates in the data in Datetime format.
        NumberofDays = len(
            MDCdataDF["DateAndTime"].dt.date.unique())  # to pass into Daily analysis number of days in data
        latestDay = str(MDCdataDF.loc[0, "DateAndTime"].date())  # to pass into Daily analysis
        LastLeg = max(MDCdataDF["Flight Leg No"])  # Latest Leg in the data
        MDCdataArray = MDCdataDF.to_numpy()  # converting to numpy to work with arrays

        # MDCMessagesDF = pd.read_csv(MDCMessagesURL_path, encoding="utf8")  # bring messages and inputs into a Dataframe
        MDCMessagesDF = connect_database_MDCmessagesInputs()
        MDCMessagesArray = MDCMessagesDF.to_numpy()  # converting to numpy to work with arrays
        ShapeMDCMessagesArray = MDCMessagesArray.shape  # tuple of the shape of the MDC message data (#rows, #columns)

        # TopMessagesDF = pd.read_csv(TopMessagesURL_path)  # bring messages and inputs into a Dataframe
        TopMessagesDF = connect_database_TopMessagesSheet()
        TopMessagesArray = TopMessagesDF.to_numpy()  # converting to numpy to work with arrays

        UniqueSerialNumArray = []

        if (airline_operator.upper() == "SKW"):
            airline_id = 101
        Flagsreport = 1  # this is here to initialize the variable. user must start report by choosing Newreport = True
        # Flagsreport: int, AircraftSN: str, , newreport: int, CurrentFlightPhaseEnabled: int
        if (include_current_message == 1):
            CurrentFlightPhaseEnabled = 1  # 1 includes current phase i.e. include
        else:
            CurrentFlightPhaseEnabled = 0  # 0 does not include current phase i.e. exclude

        MaxAllowedOccurances = occurences  # flag for Total number of occurences -> T
        MaxAllowedConsecLegs = legs  # flag for consecutive legs -> CF
        MaxAllowedConsecDays = consecutiveDays
        if intermittent > 9:
            MaxAllowedIntermittent = 9  # flag for intermittent values ->IM
        else:
            MaxAllowedIntermittent = intermittent  # flag for intermittent values ->IM
        # Bcode = equationID
        newreport = True  # set a counter variable to bring back it to false

        if (analysisType.lower() == "history"):
            # global UniqueSerialNumArray

            HistoryanalysisDF = MDCdataDF
            ShapeHistoryanalysisDF = HistoryanalysisDF.shape  # tuple of the shape of the history data (#rows, #columns)
            HistoryanalysisArray = MDCdataArray
            NumAC = HistoryanalysisDF["Aircraft"].nunique()  # number of unique aircraft SN in the data
            UniqueSerialNumArray = HistoryanalysisDF.Aircraft.unique()  # unique aircraft values
            SerialNumFreqSeries = HistoryanalysisDF.Aircraft.value_counts()  # the index of this var contains the AC with the most occurrences
            MaxOfAnAC = SerialNumFreqSeries[0]  # the freq series sorts in descending order, max value is top

            # Define the arrays as numpy
            MDCeqns_array = np.empty((MaxOfAnAC, NumAC), object)  # MDC messages for each AC stored in one array
            MDCDates_array = np.empty((MaxOfAnAC, NumAC), object)  # Dates for a message for each AC stored in one array
            MDCLegs_array = np.empty((MaxOfAnAC, NumAC),
                                     object)  # Flight Legs for a message for each AC stored in one array
            MDCIntermittent_array = np.empty((MaxOfAnAC, NumAC),
                                             object)  # stores the intermittence values for each message of each array
            FourDigATA_array = np.empty((MaxOfAnAC, NumAC), object)  # stores the 4digATAs of each message in one array
            TwoDigATA_array = np.empty((MaxOfAnAC, NumAC), object)  # stores the 2digATAs of each message in one array
            global MDCeqns_arrayforgraphing
            MDCeqns_arrayforgraphing = np.empty((MaxOfAnAC, NumAC),
                                                object)  # MDC messages for each AC stored in an array for graphing, due to current messages issue

            if CurrentFlightPhaseEnabled == 1:  # Show all, current and history
                MDCFlightPhase_array = np.ones((MaxOfAnAC, NumAC), int)
            elif CurrentFlightPhaseEnabled == 0:  # Only show history
                MDCFlightPhase_array = np.empty((MaxOfAnAC, NumAC), object)

            Messages_LastRow = ShapeMDCMessagesArray[0]  # taken from the shape of the array
            Flags_array = np.empty((Messages_LastRow, NumAC), object)
            FlightLegsEx = 'Flight legs above 32,600 for the following A/C: '  # at 32767 the DCU does not incrementmore the flight counter, so the MDC gets data for the same 32767 over and over until the limit of MDC logs per flight leg is reached (20 msgs per leg), when reached the MDC stops storing data since it gets always the same 32767
            TotalOccurances_array = np.empty((Messages_LastRow, NumAC), int)
            ConsecutiveDays_array = np.empty((Messages_LastRow, NumAC), int)
            ConsecutiveLegs_array = np.empty((Messages_LastRow, NumAC), int)
            IntermittentInLeg_array = np.empty((Messages_LastRow, NumAC), int)

            # 2D array looping, columns (SNcounter) rows (MDCsheetcounter)
            for SNCounter in range(0, NumAC):  # start counter over each aircraft (columns)

                MDCArrayCounter = 0  # rows of each different array

                for MDCsheetCounter in range(0, ShapeHistoryanalysisDF[0]):  # counter over each entry  (rows)

                    # If The Serial number on the historyanalysisarray matches the current Serial Number, copy
                    if HistoryanalysisArray[MDCsheetCounter, 0] == UniqueSerialNumArray[SNCounter]:
                        # Serial numbers match, record information
                        #       SNcounter -->
                        # format for these arrays :   | AC1 | AC2 | AC3 |.... | NumAC
                        # MDCarraycounter(vertically)| xx | xx | xx |...
                        MDCeqns_array[MDCArrayCounter, SNCounter] = HistoryanalysisArray[MDCsheetCounter, 14]
                        MDCDates_array[MDCArrayCounter, SNCounter] = HistoryanalysisArray[MDCsheetCounter, 8]
                        MDCLegs_array[MDCArrayCounter, SNCounter] = HistoryanalysisArray[MDCsheetCounter, 2]
                        MDCIntermittent_array[MDCArrayCounter, SNCounter] = HistoryanalysisArray[MDCsheetCounter, 13]

                        if HistoryanalysisArray[MDCsheetCounter, 11]:  # populating counts array
                            FourDigATA_array[MDCArrayCounter, SNCounter] = HistoryanalysisArray[MDCsheetCounter, 5]
                            TwoDigATA_array[MDCArrayCounter, SNCounter] = HistoryanalysisArray[MDCsheetCounter, 3]

                        if CurrentFlightPhaseEnabled == 0:  # populating the empty array
                            MDCFlightPhase_array[MDCArrayCounter, SNCounter] = HistoryanalysisArray[MDCsheetCounter, 11]

                        MDCArrayCounter = MDCArrayCounter + 1

                # arrays with the same size as the MDC messages sheet (3519) checks if each message exists in each ac
                for MessagessheetCounter in range(0, Messages_LastRow):

                    # Initialize Counts, etc

                    # Total Occurances
                    eqnCount = 0

                    # Consecutive Days
                    ConsecutiveDays = 0
                    MaxConsecutiveDays = 0
                    tempDate = pd.to_datetime(latestDay)
                    DaysCount = 0

                    # Consecutive Legs
                    ConsecutiveLegs = 0
                    MaxConsecutiveLegs = 0
                    tempLeg = LastLeg

                    # Intermittent
                    IntermittentFlightLegs = 0

                    MDCArrayCounter = 0

                    while MDCArrayCounter < MaxOfAnAC:
                        if MDCeqns_array[MDCArrayCounter, SNCounter]:
                            # Not Empty, and not current                                      B code
                            if MDCeqns_array[MDCArrayCounter, SNCounter] == MDCMessagesArray[MessagessheetCounter, 12] \
                                    and MDCFlightPhase_array[MDCArrayCounter, SNCounter]:

                                MDCeqns_arrayforgraphing[MDCArrayCounter, SNCounter] = MDCeqns_array[
                                    MDCArrayCounter, SNCounter]

                                # Total Occurances
                                # Count this as 1 occurance
                                eqnCount = eqnCount + 1

                                # Consecutive Days
                                currentdate = pd.to_datetime(MDCDates_array[MDCArrayCounter, SNCounter])
                                # first date and fivedaysafter
                                # if a flag was raised in the previous count, it has to be reset and a new fivedaysafter is declared
                                if eqnCount == 1 or flag == True:
                                    flag = False
                                    FiveDaysAfter = currentdate + datetime.timedelta(5)

                                # by checking when its even or odd, we can check if the message occurred twice in 5 days
                                # if the second time it occurred is below fivedaysafter, flag is true
                                # if the second time it occurred is greater than fivedaysafter, flag is false
                                # if eqncount is odd, flag is false and new fivedaysafter is declared
                                if (eqnCount % 2) == 0:
                                    if currentdate <= FiveDaysAfter:
                                        flag = True
                                    else:
                                        flag = False
                                else:
                                    FiveDaysAfter = currentdate + datetime.timedelta(5)
                                    flag = False

                                if currentdate.day == tempDate.day \
                                        and currentdate.month == tempDate.month \
                                        and currentdate.year == tempDate.year:

                                    DaysCount = 1  # 1 because consecutive means 1 day since it occured
                                    tempDate = tempDate - datetime.timedelta(1)
                                    ConsecutiveDays = ConsecutiveDays + 1

                                    if ConsecutiveDays >= MaxConsecutiveDays:
                                        MaxConsecutiveDays = ConsecutiveDays

                                elif MDCDates_array[MDCArrayCounter, SNCounter] < tempDate:

                                    # If not consecutive, start over
                                    if ConsecutiveDays >= MaxConsecutiveDays:
                                        MaxConsecutiveDays = ConsecutiveDays

                                    ConsecutiveDays = 1
                                    # Days count is the delta between this current date and the previous temp date
                                    DaysCount += abs(tempDate - currentdate).days + 1
                                    tempDate = currentdate - datetime.timedelta(1)

                                # Consecutive Legs
                                if MDCLegs_array[MDCArrayCounter, SNCounter] == tempLeg:

                                    tempLeg = tempLeg - 1
                                    ConsecutiveLegs = ConsecutiveLegs + 1

                                    if ConsecutiveLegs > MaxConsecutiveLegs:
                                        MaxConsecutiveLegs = ConsecutiveLegs

                                else:

                                    # If not consecutive, start over
                                    ConsecutiveLegs = 1
                                    tempLeg = MDCLegs_array[MDCArrayCounter, SNCounter]

                                # Intermittent
                                # Taking the maximum intermittent value
                                x = MDCIntermittent_array[MDCArrayCounter, SNCounter]
                                # if isinstance(x, numbers.Number) and MDCIntermittent_array[MDCArrayCounter, SNCounter] > IntermittentFlightLegs:
                                if MDCIntermittent_array[MDCArrayCounter, SNCounter] > IntermittentFlightLegs:
                                    IntermittentFlightLegs = MDCIntermittent_array[MDCArrayCounter, SNCounter]
                                # End if Intermittent numeric check

                                # Other
                                # Check that the legs is not over the given limit
                                Flags_array[MessagessheetCounter, SNCounter] = ''
                                if MDCLegs_array[MDCArrayCounter, SNCounter] > 32600:
                                    FlightLegsEx = FlightLegsEx + str(UniqueSerialNumArray[SNCounter]) + ' (' + str(
                                        MDCLegs_array[MDCArrayCounter, SNCounter]) + ')' + ' '
                                # End if Legs flag

                                # Check for Other Flags
                                if MDCMessagesArray[MessagessheetCounter, 13]:
                                    # Immediate (occurrance flag in excel MDC Messages inputs sheet) - JAM RELATED FLAGS
                                    if MDCMessagesArray[MessagessheetCounter, 13] == 1 and MDCMessagesArray[
                                        MessagessheetCounter, 14] == 0:
                                        # Immediate Flag required
                                        # lIST OF MESSAGES TO BE FLAGGED AS SOON AS POSTED
                                        # ["B1-309178","B1-309179","B1-309180","B1-060044","B1-060045","B1-007973",
                                        # "B1-060017","B1-006551","B1-240885","B1-006552","B1-006553","B1-006554",
                                        # "B1-006555","B1-007798","B1-007772","B1-240938","B1-007925","B1-007905",
                                        # "B1-007927","B1-007915","B1-007926","B1-007910","B1-007928","B1-007920"]
                                        Flags_array[MessagessheetCounter, SNCounter] = str(
                                            MDCMessagesArray[MessagessheetCounter, 12]) + " occured at least once."


                                    elif MDCMessagesArray[MessagessheetCounter, 13] == 2 and \
                                            MDCMessagesArray[MessagessheetCounter, 14] == 5 and \
                                            flag == True:
                                        # Triggered twice in 5 days
                                        # "B1-008350","B1-008351","B1-008360","B1-008361"
                                        Flags_array[MessagessheetCounter, SNCounter] = str(MDCMessagesArray[
                                                                                               MessagessheetCounter, 12]) + " occured at least twice in 5 days. "
                            MDCArrayCounter += 1

                        else:
                            MDCArrayCounter = MaxOfAnAC

                            # Next MDCArray Counter

                    TotalOccurances_array[MessagessheetCounter, SNCounter] = eqnCount
                    ConsecutiveDays_array[MessagessheetCounter, SNCounter] = MaxConsecutiveDays
                    ConsecutiveLegs_array[MessagessheetCounter, SNCounter] = MaxConsecutiveLegs
                    IntermittentInLeg_array[MessagessheetCounter, SNCounter] = IntermittentFlightLegs
                # Next MessagessheetCounter
            # Next SNCounter

            MAINtable_array_temp = np.empty((1, 18), object)  # 18 because its history #????????
            currentRow = 0
            MAINtable_array = []
            for SNCounter in range(0, NumAC):
                for EqnCounter in range(0, Messages_LastRow):

                    # Continue with Report
                    if TotalOccurances_array[EqnCounter, SNCounter] >= MaxAllowedOccurances \
                            or ConsecutiveDays_array[EqnCounter, SNCounter] >= MaxAllowedConsecDays \
                            or ConsecutiveLegs_array[EqnCounter, SNCounter] >= MaxAllowedConsecLegs \
                            or IntermittentInLeg_array[EqnCounter, SNCounter] >= MaxAllowedIntermittent \
                            or Flags_array[EqnCounter, SNCounter]:

                        # Populate Flags Array
                        if TotalOccurances_array[EqnCounter, SNCounter] >= MaxAllowedOccurances:
                            Flags_array[EqnCounter, SNCounter] = Flags_array[
                                                                     EqnCounter, SNCounter] + "Total occurances exceeded " + str(
                                MaxAllowedOccurances) + " occurances. "

                        if ConsecutiveDays_array[EqnCounter, SNCounter] >= MaxAllowedConsecDays:
                            Flags_array[EqnCounter, SNCounter] = Flags_array[
                                                                     EqnCounter, SNCounter] + "Maximum consecutive days exceeded " + str(
                                MaxAllowedConsecDays) + " days. "

                        if ConsecutiveLegs_array[EqnCounter, SNCounter] >= MaxAllowedConsecLegs:
                            Flags_array[EqnCounter, SNCounter] = Flags_array[
                                                                     EqnCounter, SNCounter] + "Maximum consecutive flight legs exceeded " + str(
                                MaxAllowedConsecLegs) + " flight legs. "

                        if IntermittentInLeg_array[EqnCounter, SNCounter] >= MaxAllowedIntermittent:
                            Flags_array[EqnCounter, SNCounter] = Flags_array[
                                                                     EqnCounter, SNCounter] + "Maximum intermittent occurances for one flight leg exceeded " + str(
                                MaxAllowedIntermittent) + " occurances. "

                        # populating the final array (Table)
                        MAINtable_array_temp[0, 0] = UniqueSerialNumArray[SNCounter]
                        MAINtable_array_temp[0, 1] = MDCMessagesArray[EqnCounter, 8]
                        MAINtable_array_temp[0, 2] = MDCMessagesArray[EqnCounter, 4]
                        MAINtable_array_temp[0, 3] = MDCMessagesArray[EqnCounter, 0]
                        MAINtable_array_temp[0, 4] = MDCMessagesArray[EqnCounter, 1]
                        MAINtable_array_temp[0, 5] = MDCMessagesArray[EqnCounter, 12]
                        MAINtable_array_temp[0, 6] = MDCMessagesArray[EqnCounter, 7]
                        MAINtable_array_temp[0, 7] = MDCMessagesArray[EqnCounter, 11]
                        MAINtable_array_temp[0, 8] = TotalOccurances_array[EqnCounter, SNCounter]
                        MAINtable_array_temp[0, 9] = ConsecutiveDays_array[EqnCounter, SNCounter]
                        MAINtable_array_temp[0, 10] = ConsecutiveLegs_array[EqnCounter, SNCounter]
                        MAINtable_array_temp[0, 11] = IntermittentInLeg_array[EqnCounter, SNCounter]
                        MAINtable_array_temp[0, 12] = Flags_array[EqnCounter, SNCounter]

                        # if the input is empty set the priority to 4
                        if MDCMessagesArray[EqnCounter, 15] == 0:
                            MAINtable_array_temp[0, 13] = 4
                        else:
                            MAINtable_array_temp[0, 13] = MDCMessagesArray[EqnCounter, 15]

                        # For B1-006424 & B1-006430 Could MDC Trend tool assign Priority 3 if logged on A/C below 10340, 15317. Priority 1 if logged on 10340, 15317, 19001 and up
                        if MDCMessagesArray[EqnCounter, 12] == "B1-006424" or MDCMessagesArray[
                            EqnCounter, 12] == "B1-006430":
                            if int(UniqueSerialNumArray[SNCounter]) <= 10340 and int(
                                    UniqueSerialNumArray[SNCounter]) > 10000:
                                MAINtable_array_temp[0, 13] = 3
                            elif int(UniqueSerialNumArray[SNCounter]) > 10340 and int(
                                    UniqueSerialNumArray[SNCounter]) < 11000:
                                MAINtable_array_temp[0, 13] = 1
                            elif int(UniqueSerialNumArray[SNCounter]) <= 15317 and int(
                                    UniqueSerialNumArray[SNCounter]) > 15000:
                                MAINtable_array_temp[0, 13] = 3
                            elif int(UniqueSerialNumArray[SNCounter]) > 15317 and int(
                                    UniqueSerialNumArray[SNCounter]) < 16000:
                                MAINtable_array_temp[0, 13] = 1
                            elif int(UniqueSerialNumArray[SNCounter]) >= 19001 and int(
                                    UniqueSerialNumArray[SNCounter]) < 20000:
                                MAINtable_array_temp[0, 13] = 1

                        # check the content of MHIRJ ISE recommendation and add to array
                        if MDCMessagesArray[EqnCounter, 16] == 0:
                            MAINtable_array_temp[0, 15] = ""
                        else:
                            MAINtable_array_temp[0, 15] = MDCMessagesArray[EqnCounter, 16]

                        # check content of "additional"
                        if MDCMessagesArray[EqnCounter, 17] == 0:
                            MAINtable_array_temp[0, 16] = ""
                        else:
                            MAINtable_array_temp[0, 16] = MDCMessagesArray[EqnCounter, 17]

                        # check content of "MHIRJ Input"
                        if MDCMessagesArray[EqnCounter, 18] == 0:
                            MAINtable_array_temp[0, 17] = ""
                        else:
                            MAINtable_array_temp[0, 17] = MDCMessagesArray[EqnCounter, 18]

                        # Check for the equation in the Top Messages sheet
                        TopCounter = 0
                        Top_LastRow = TopMessagesArray.shape[0]
                        while TopCounter < Top_LastRow:

                            # Look for the flagged equation in the Top Messages Sheet
                            if MDCMessagesArray[EqnCounter][12] == TopMessagesArray[TopCounter, 4]:

                                # Found the equation in the Top Messages Sheet. Put the information in the last column
                                MAINtable_array_temp[0, 14] = "Known Nuissance: " + str(
                                    TopMessagesArray[TopCounter, 13]) \
                                                              + " / In-Service Document: " + str(
                                    TopMessagesArray[TopCounter, 11]) \
                                                              + " / FIM Task: " + str(TopMessagesArray[TopCounter, 10]) \
                                                              + " / Remarks: " + str(TopMessagesArray[TopCounter, 14])

                                # Not need to keep looking
                                TopCounter = TopMessagesArray.shape[0]

                            else:
                                # Not equal, go to next equation
                                MAINtable_array_temp[0, 14] = ""
                                TopCounter += 1
                        # End while

                        if currentRow == 0:
                            MAINtable_array = np.array(MAINtable_array_temp)
                        else:
                            MAINtable_array = np.append(MAINtable_array, MAINtable_array_temp, axis=0)
                        # End if Build MAINtable_array

                        # Move to next Row on Main page for next flag
                        currentRow = currentRow + 1
            TitlesArrayHistory = ["AC SN", "EICAS Message", "MDC Message", "LRU", "ATA", "B1-Equation", "Type",
                                  "Equation Description", "Total Occurences", "Consective Days", "Consecutive FL",
                                  "Intermittent", "Reason(s) for flag", "Priority",
                                  "Known Top Message - Recommended Documents",
                                  "MHIRJ ISE Recommendation", "Additional Comments", "MHIRJ ISE Input"]

            # Converts the Numpy Array to Dataframe to manipulate
            # pd.set_option('display.max_rows', None)
            # Main table
            global OutputTableHistory
            OutputTableHistory = pd.DataFrame(data=MAINtable_array, columns=TitlesArrayHistory).fillna(" ").sort_values(
                by=["Type", "Priority"])
            OutputTableHistory = OutputTableHistory.merge(AircraftTailPairDF, on="AC SN")  # Tail # added
            OutputTableHistory = OutputTableHistory[
                ["Tail#", "AC SN", "EICAS Message", "MDC Message", "LRU", "ATA", "B1-Equation", "Type",
                 "Equation Description", "Total Occurences", "Consective Days", "Consecutive FL",
                 "Intermittent", "Reason(s) for flag", "Priority", "Known Top Message - Recommended Documents",
                 "MHIRJ ISE Recommendation", "Additional Comments",
                 "MHIRJ ISE Input"]]  # Tail# added to output table which means that column order has to be re ordered
            # OutputTableHistory.to_csv("OutputTableHistory.csv")
            # OutputTableHistory_json = OutputTableHistory.to_json(orient = 'records')
            # return OutputTableHistory_json

            # Get the list of JAM Messages.
            listofJamMessages = list()
            all_jam_messages = connect_to_fetch_all_jam_messages()
            for each_jam_message in all_jam_messages['Jam_Message']:
                listofJamMessages.append(each_jam_message)
            # print(listofJamMessages)

            # HIGHLIGHT function starts here
            OutputTableHistory = OutputTableHistory.assign(
                is_jam=lambda dataframe: dataframe['B1-Equation'].map(
                    lambda c: True if c in listofJamMessages else False)
            )
            print(OutputTableHistory)
            prev_history = OutputTableHistory
            # OutputTableHistory_json = OutputTableHistory.to_json(orient='records')
            # return OutputTableHistory_json

    ## Current dates computation
    if not (curr_fromDate is None and curr_toDate is None):
        MDCdataDF = connect_database_MDCdata(ata, exclude_EqID, airline_operator, include_current_message,
                                             curr_fromDate, curr_toDate)

        # print(":: MDC DF CURR ::")
        # print(MDCdataDF)

        # Date formatting
        MDCdataDF["DateAndTime"] = pd.to_datetime(MDCdataDF["DateAndTime"])
        # print(MDCdataDF["DateAndTime"])
        MDCdataDF["Flight Leg No"].fillna(value=0.0,
                                          inplace=True)  # Null values preprocessing - if 0 = Currentflightphase
        # print(MDCdataDF["Flight Leg No"])
        MDCdataDF["Flight Phase"].fillna(False, inplace=True)  # NuCell values preprocessing for currentflightphase
        MDCdataDF["Intermittent"].fillna(value=-1, inplace=True)  # Null values preprocessing for currentflightphase
        MDCdataDF["Intermittent"].replace(to_replace=">", value="9",
                                          inplace=True)  # > represents greater than 8 Intermittent values
        MDCdataDF["Intermittent"] = MDCdataDF["Intermittent"].astype(int)  # cast type to int

        MDCdataDF["Aircraft"] = MDCdataDF["Aircraft"].str.replace('AC', '')
        MDCdataDF.fillna(value=" ", inplace=True)  # replacing all REMAINING null values to a blank string
        MDCdataDF.sort_values(by="DateAndTime", ascending=False, inplace=True, ignore_index=True)

        AircraftTailPairDF = MDCdataDF[["Aircraft", "Tail#"]].drop_duplicates(
            ignore_index=True)  # unique pairs of AC SN and Tail# for use in analysis
        AircraftTailPairDF.columns = ["AC SN", "Tail#"]  # re naming the columns to match History/Daily analysis output

        DatesinData = MDCdataDF["DateAndTime"].dt.date.unique()  # these are the dates in the data in Datetime format.
        NumberofDays = len(
            MDCdataDF["DateAndTime"].dt.date.unique())  # to pass into Daily analysis number of days in data
        latestDay = str(MDCdataDF.loc[0, "DateAndTime"].date())  # to pass into Daily analysis
        LastLeg = max(MDCdataDF["Flight Leg No"])  # Latest Leg in the data
        MDCdataArray = MDCdataDF.to_numpy()  # converting to numpy to work with arrays

        # MDCMessagesDF = pd.read_csv(MDCMessagesURL_path, encoding="utf8")  # bring messages and inputs into a Dataframe
        MDCMessagesDF = connect_database_MDCmessagesInputs()
        MDCMessagesArray = MDCMessagesDF.to_numpy()  # converting to numpy to work with arrays
        ShapeMDCMessagesArray = MDCMessagesArray.shape  # tuple of the shape of the MDC message data (#rows, #columns)

        # TopMessagesDF = pd.read_csv(TopMessagesURL_path)  # bring messages and inputs into a Dataframe
        TopMessagesDF = connect_database_TopMessagesSheet()
        TopMessagesArray = TopMessagesDF.to_numpy()  # converting to numpy to work with arrays

        UniqueSerialNumArray = []

        if (airline_operator.upper() == "SKW"):
            airline_id = 101
        Flagsreport = 1  # this is here to initialize the variable. user must start report by choosing Newreport = True
        # Flagsreport: int, AircraftSN: str, , newreport: int, CurrentFlightPhaseEnabled: int
        if (include_current_message == 1):
            CurrentFlightPhaseEnabled = 1  # 1 includes current phase i.e. include
        else:
            CurrentFlightPhaseEnabled = 0  # 0 does not include current phase i.e. exclude

        MaxAllowedOccurances = occurences  # flag for Total number of occurences -> T
        MaxAllowedConsecLegs = legs  # flag for consecutive legs -> CF
        MaxAllowedConsecDays = consecutiveDays
        if intermittent > 9:
            MaxAllowedIntermittent = 9  # flag for intermittent values ->IM
        else:
            MaxAllowedIntermittent = intermittent  # flag for intermittent values ->IM
        # Bcode = equationID
        newreport = True  # set a counter variable to bring back it to false

        if (analysisType.lower() == "history"):
            # global UniqueSerialNumArray

            HistoryanalysisDF = MDCdataDF
            ShapeHistoryanalysisDF = HistoryanalysisDF.shape  # tuple of the shape of the history data (#rows, #columns)
            HistoryanalysisArray = MDCdataArray
            NumAC = HistoryanalysisDF["Aircraft"].nunique()  # number of unique aircraft SN in the data
            UniqueSerialNumArray = HistoryanalysisDF.Aircraft.unique()  # unique aircraft values
            SerialNumFreqSeries = HistoryanalysisDF.Aircraft.value_counts()  # the index of this var contains the AC with the most occurrences
            MaxOfAnAC = SerialNumFreqSeries[0]  # the freq series sorts in descending order, max value is top

            # Define the arrays as numpy
            MDCeqns_array = np.empty((MaxOfAnAC, NumAC), object)  # MDC messages for each AC stored in one array
            MDCDates_array = np.empty((MaxOfAnAC, NumAC), object)  # Dates for a message for each AC stored in one array
            MDCLegs_array = np.empty((MaxOfAnAC, NumAC),
                                     object)  # Flight Legs for a message for each AC stored in one array
            MDCIntermittent_array = np.empty((MaxOfAnAC, NumAC),
                                             object)  # stores the intermittence values for each message of each array
            FourDigATA_array = np.empty((MaxOfAnAC, NumAC), object)  # stores the 4digATAs of each message in one array
            TwoDigATA_array = np.empty((MaxOfAnAC, NumAC), object)  # stores the 2digATAs of each message in one array

            MDCeqns_arrayforgraphing = np.empty((MaxOfAnAC, NumAC),
                                                object)  # MDC messages for each AC stored in an array for graphing, due to current messages issue

            if CurrentFlightPhaseEnabled == 1:  # Show all, current and history
                MDCFlightPhase_array = np.ones((MaxOfAnAC, NumAC), int)
            elif CurrentFlightPhaseEnabled == 0:  # Only show history
                MDCFlightPhase_array = np.empty((MaxOfAnAC, NumAC), object)

            Messages_LastRow = ShapeMDCMessagesArray[0]  # taken from the shape of the array
            Flags_array = np.empty((Messages_LastRow, NumAC), object)
            FlightLegsEx = 'Flight legs above 32,600 for the following A/C: '  # at 32767 the DCU does not incrementmore the flight counter, so the MDC gets data for the same 32767 over and over until the limit of MDC logs per flight leg is reached (20 msgs per leg), when reached the MDC stops storing data since it gets always the same 32767
            TotalOccurances_array = np.empty((Messages_LastRow, NumAC), int)
            ConsecutiveDays_array = np.empty((Messages_LastRow, NumAC), int)
            ConsecutiveLegs_array = np.empty((Messages_LastRow, NumAC), int)
            IntermittentInLeg_array = np.empty((Messages_LastRow, NumAC), int)

            # 2D array looping, columns (SNcounter) rows (MDCsheetcounter)
            for SNCounter in range(0, NumAC):  # start counter over each aircraft (columns)

                MDCArrayCounter = 0  # rows of each different array

                for MDCsheetCounter in range(0, ShapeHistoryanalysisDF[0]):  # counter over each entry  (rows)

                    # If The Serial number on the historyanalysisarray matches the current Serial Number, copy
                    if HistoryanalysisArray[MDCsheetCounter, 0] == UniqueSerialNumArray[SNCounter]:
                        # Serial numbers match, record information
                        #       SNcounter -->
                        # format for these arrays :   | AC1 | AC2 | AC3 |.... | NumAC
                        # MDCarraycounter(vertically)| xx | xx | xx |...
                        MDCeqns_array[MDCArrayCounter, SNCounter] = HistoryanalysisArray[MDCsheetCounter, 14]
                        MDCDates_array[MDCArrayCounter, SNCounter] = HistoryanalysisArray[MDCsheetCounter, 8]
                        MDCLegs_array[MDCArrayCounter, SNCounter] = HistoryanalysisArray[MDCsheetCounter, 2]
                        MDCIntermittent_array[MDCArrayCounter, SNCounter] = HistoryanalysisArray[MDCsheetCounter, 13]

                        if HistoryanalysisArray[MDCsheetCounter, 11]:  # populating counts array
                            FourDigATA_array[MDCArrayCounter, SNCounter] = HistoryanalysisArray[MDCsheetCounter, 5]
                            TwoDigATA_array[MDCArrayCounter, SNCounter] = HistoryanalysisArray[MDCsheetCounter, 3]

                        if CurrentFlightPhaseEnabled == 0:  # populating the empty array
                            MDCFlightPhase_array[MDCArrayCounter, SNCounter] = HistoryanalysisArray[MDCsheetCounter, 11]

                        MDCArrayCounter = MDCArrayCounter + 1

                # arrays with the same size as the MDC messages sheet (3519) checks if each message exists in each ac
                for MessagessheetCounter in range(0, Messages_LastRow):

                    # Initialize Counts, etc

                    # Total Occurances
                    eqnCount = 0

                    # Consecutive Days
                    ConsecutiveDays = 0
                    MaxConsecutiveDays = 0
                    tempDate = pd.to_datetime(latestDay)
                    DaysCount = 0

                    # Consecutive Legs
                    ConsecutiveLegs = 0
                    MaxConsecutiveLegs = 0
                    tempLeg = LastLeg

                    # Intermittent
                    IntermittentFlightLegs = 0

                    MDCArrayCounter = 0

                    while MDCArrayCounter < MaxOfAnAC:
                        if MDCeqns_array[MDCArrayCounter, SNCounter]:
                            # Not Empty, and not current                                      B code
                            if MDCeqns_array[MDCArrayCounter, SNCounter] == MDCMessagesArray[MessagessheetCounter, 12] \
                                    and MDCFlightPhase_array[MDCArrayCounter, SNCounter]:

                                MDCeqns_arrayforgraphing[MDCArrayCounter, SNCounter] = MDCeqns_array[
                                    MDCArrayCounter, SNCounter]

                                # Total Occurances
                                # Count this as 1 occurance
                                eqnCount = eqnCount + 1

                                # Consecutive Days
                                currentdate = pd.to_datetime(MDCDates_array[MDCArrayCounter, SNCounter])
                                # first date and fivedaysafter
                                # if a flag was raised in the previous count, it has to be reset and a new fivedaysafter is declared
                                if eqnCount == 1 or flag == True:
                                    flag = False
                                    FiveDaysAfter = currentdate + datetime.timedelta(5)

                                # by checking when its even or odd, we can check if the message occurred twice in 5 days
                                # if the second time it occurred is below fivedaysafter, flag is true
                                # if the second time it occurred is greater than fivedaysafter, flag is false
                                # if eqncount is odd, flag is false and new fivedaysafter is declared
                                if (eqnCount % 2) == 0:
                                    if currentdate <= FiveDaysAfter:
                                        flag = True
                                    else:
                                        flag = False
                                else:
                                    FiveDaysAfter = currentdate + datetime.timedelta(5)
                                    flag = False

                                if currentdate.day == tempDate.day \
                                        and currentdate.month == tempDate.month \
                                        and currentdate.year == tempDate.year:

                                    DaysCount = 1  # 1 because consecutive means 1 day since it occured
                                    tempDate = tempDate - datetime.timedelta(1)
                                    ConsecutiveDays = ConsecutiveDays + 1

                                    if ConsecutiveDays >= MaxConsecutiveDays:
                                        MaxConsecutiveDays = ConsecutiveDays

                                elif MDCDates_array[MDCArrayCounter, SNCounter] < tempDate:

                                    # If not consecutive, start over
                                    if ConsecutiveDays >= MaxConsecutiveDays:
                                        MaxConsecutiveDays = ConsecutiveDays

                                    ConsecutiveDays = 1
                                    # Days count is the delta between this current date and the previous temp date
                                    DaysCount += abs(tempDate - currentdate).days + 1
                                    tempDate = currentdate - datetime.timedelta(1)

                                # Consecutive Legs
                                if MDCLegs_array[MDCArrayCounter, SNCounter] == tempLeg:

                                    tempLeg = tempLeg - 1
                                    ConsecutiveLegs = ConsecutiveLegs + 1

                                    if ConsecutiveLegs > MaxConsecutiveLegs:
                                        MaxConsecutiveLegs = ConsecutiveLegs

                                else:

                                    # If not consecutive, start over
                                    ConsecutiveLegs = 1
                                    tempLeg = MDCLegs_array[MDCArrayCounter, SNCounter]

                                # Intermittent
                                # Taking the maximum intermittent value
                                x = MDCIntermittent_array[MDCArrayCounter, SNCounter]
                                # if isinstance(x, numbers.Number) and MDCIntermittent_array[MDCArrayCounter, SNCounter] > IntermittentFlightLegs:
                                if MDCIntermittent_array[MDCArrayCounter, SNCounter] > IntermittentFlightLegs:
                                    IntermittentFlightLegs = MDCIntermittent_array[MDCArrayCounter, SNCounter]
                                # End if Intermittent numeric check

                                # Other
                                # Check that the legs is not over the given limit
                                Flags_array[MessagessheetCounter, SNCounter] = ''
                                if MDCLegs_array[MDCArrayCounter, SNCounter] > 32600:
                                    FlightLegsEx = FlightLegsEx + str(UniqueSerialNumArray[SNCounter]) + ' (' + str(
                                        MDCLegs_array[MDCArrayCounter, SNCounter]) + ')' + ' '
                                # End if Legs flag

                                # Check for Other Flags
                                if MDCMessagesArray[MessagessheetCounter, 13]:
                                    # Immediate (occurrance flag in excel MDC Messages inputs sheet) - JAM RELATED FLAGS
                                    if MDCMessagesArray[MessagessheetCounter, 13] == 1 and MDCMessagesArray[
                                        MessagessheetCounter, 14] == 0:
                                        # Immediate Flag required
                                        # lIST OF MESSAGES TO BE FLAGGED AS SOON AS POSTED
                                        # ["B1-309178","B1-309179","B1-309180","B1-060044","B1-060045","B1-007973",
                                        # "B1-060017","B1-006551","B1-240885","B1-006552","B1-006553","B1-006554",
                                        # "B1-006555","B1-007798","B1-007772","B1-240938","B1-007925","B1-007905",
                                        # "B1-007927","B1-007915","B1-007926","B1-007910","B1-007928","B1-007920"]
                                        Flags_array[MessagessheetCounter, SNCounter] = str(
                                            MDCMessagesArray[MessagessheetCounter, 12]) + " occured at least once."


                                    elif MDCMessagesArray[MessagessheetCounter, 13] == 2 and \
                                            MDCMessagesArray[MessagessheetCounter, 14] == 5 and \
                                            flag == True:
                                        # Triggered twice in 5 days
                                        # "B1-008350","B1-008351","B1-008360","B1-008361"
                                        Flags_array[MessagessheetCounter, SNCounter] = str(MDCMessagesArray[
                                                                                               MessagessheetCounter, 12]) + " occured at least twice in 5 days. "
                            MDCArrayCounter += 1

                        else:
                            MDCArrayCounter = MaxOfAnAC

                            # Next MDCArray Counter

                    TotalOccurances_array[MessagessheetCounter, SNCounter] = eqnCount
                    ConsecutiveDays_array[MessagessheetCounter, SNCounter] = MaxConsecutiveDays
                    ConsecutiveLegs_array[MessagessheetCounter, SNCounter] = MaxConsecutiveLegs
                    IntermittentInLeg_array[MessagessheetCounter, SNCounter] = IntermittentFlightLegs
                # Next MessagessheetCounter
            # Next SNCounter

            MAINtable_array_temp = np.empty((1, 18), object)  # 18 because its history #????????
            currentRow = 0
            MAINtable_array = []
            for SNCounter in range(0, NumAC):
                for EqnCounter in range(0, Messages_LastRow):

                    # Continue with Report
                    if TotalOccurances_array[EqnCounter, SNCounter] >= MaxAllowedOccurances \
                            or ConsecutiveDays_array[EqnCounter, SNCounter] >= MaxAllowedConsecDays \
                            or ConsecutiveLegs_array[EqnCounter, SNCounter] >= MaxAllowedConsecLegs \
                            or IntermittentInLeg_array[EqnCounter, SNCounter] >= MaxAllowedIntermittent \
                            or Flags_array[EqnCounter, SNCounter]:

                        # Populate Flags Array
                        if TotalOccurances_array[EqnCounter, SNCounter] >= MaxAllowedOccurances:
                            Flags_array[EqnCounter, SNCounter] = Flags_array[
                                                                     EqnCounter, SNCounter] + "Total occurances exceeded " + str(
                                MaxAllowedOccurances) + " occurances. "

                        if ConsecutiveDays_array[EqnCounter, SNCounter] >= MaxAllowedConsecDays:
                            Flags_array[EqnCounter, SNCounter] = Flags_array[
                                                                     EqnCounter, SNCounter] + "Maximum consecutive days exceeded " + str(
                                MaxAllowedConsecDays) + " days. "

                        if ConsecutiveLegs_array[EqnCounter, SNCounter] >= MaxAllowedConsecLegs:
                            Flags_array[EqnCounter, SNCounter] = Flags_array[
                                                                     EqnCounter, SNCounter] + "Maximum consecutive flight legs exceeded " + str(
                                MaxAllowedConsecLegs) + " flight legs. "

                        if IntermittentInLeg_array[EqnCounter, SNCounter] >= MaxAllowedIntermittent:
                            Flags_array[EqnCounter, SNCounter] = Flags_array[
                                                                     EqnCounter, SNCounter] + "Maximum intermittent occurances for one flight leg exceeded " + str(
                                MaxAllowedIntermittent) + " occurances. "

                        # populating the final array (Table)
                        MAINtable_array_temp[0, 0] = UniqueSerialNumArray[SNCounter]
                        MAINtable_array_temp[0, 1] = MDCMessagesArray[EqnCounter, 8]
                        MAINtable_array_temp[0, 2] = MDCMessagesArray[EqnCounter, 4]
                        MAINtable_array_temp[0, 3] = MDCMessagesArray[EqnCounter, 0]
                        MAINtable_array_temp[0, 4] = MDCMessagesArray[EqnCounter, 1]
                        MAINtable_array_temp[0, 5] = MDCMessagesArray[EqnCounter, 12]
                        MAINtable_array_temp[0, 6] = MDCMessagesArray[EqnCounter, 7]
                        MAINtable_array_temp[0, 7] = MDCMessagesArray[EqnCounter, 11]
                        MAINtable_array_temp[0, 8] = TotalOccurances_array[EqnCounter, SNCounter]
                        MAINtable_array_temp[0, 9] = ConsecutiveDays_array[EqnCounter, SNCounter]
                        MAINtable_array_temp[0, 10] = ConsecutiveLegs_array[EqnCounter, SNCounter]
                        MAINtable_array_temp[0, 11] = IntermittentInLeg_array[EqnCounter, SNCounter]
                        MAINtable_array_temp[0, 12] = Flags_array[EqnCounter, SNCounter]

                        # if the input is empty set the priority to 4
                        if MDCMessagesArray[EqnCounter, 15] == 0:
                            MAINtable_array_temp[0, 13] = 4
                        else:
                            MAINtable_array_temp[0, 13] = MDCMessagesArray[EqnCounter, 15]

                        # For B1-006424 & B1-006430 Could MDC Trend tool assign Priority 3 if logged on A/C below 10340, 15317. Priority 1 if logged on 10340, 15317, 19001 and up
                        if MDCMessagesArray[EqnCounter, 12] == "B1-006424" or MDCMessagesArray[
                            EqnCounter, 12] == "B1-006430":
                            if int(UniqueSerialNumArray[SNCounter]) <= 10340 and int(
                                    UniqueSerialNumArray[SNCounter]) > 10000:
                                MAINtable_array_temp[0, 13] = 3
                            elif int(UniqueSerialNumArray[SNCounter]) > 10340 and int(
                                    UniqueSerialNumArray[SNCounter]) < 11000:
                                MAINtable_array_temp[0, 13] = 1
                            elif int(UniqueSerialNumArray[SNCounter]) <= 15317 and int(
                                    UniqueSerialNumArray[SNCounter]) > 15000:
                                MAINtable_array_temp[0, 13] = 3
                            elif int(UniqueSerialNumArray[SNCounter]) > 15317 and int(
                                    UniqueSerialNumArray[SNCounter]) < 16000:
                                MAINtable_array_temp[0, 13] = 1
                            elif int(UniqueSerialNumArray[SNCounter]) >= 19001 and int(
                                    UniqueSerialNumArray[SNCounter]) < 20000:
                                MAINtable_array_temp[0, 13] = 1

                        # check the content of MHIRJ ISE recommendation and add to array
                        if MDCMessagesArray[EqnCounter, 16] == 0:
                            MAINtable_array_temp[0, 15] = ""
                        else:
                            MAINtable_array_temp[0, 15] = MDCMessagesArray[EqnCounter, 16]

                        # check content of "additional"
                        if MDCMessagesArray[EqnCounter, 17] == 0:
                            MAINtable_array_temp[0, 16] = ""
                        else:
                            MAINtable_array_temp[0, 16] = MDCMessagesArray[EqnCounter, 17]

                        # check content of "MHIRJ Input"
                        if MDCMessagesArray[EqnCounter, 18] == 0:
                            MAINtable_array_temp[0, 17] = ""
                        else:
                            MAINtable_array_temp[0, 17] = MDCMessagesArray[EqnCounter, 18]

                        # Check for the equation in the Top Messages sheet
                        TopCounter = 0
                        Top_LastRow = TopMessagesArray.shape[0]
                        while TopCounter < Top_LastRow:

                            # Look for the flagged equation in the Top Messages Sheet
                            if MDCMessagesArray[EqnCounter][12] == TopMessagesArray[TopCounter, 4]:

                                # Found the equation in the Top Messages Sheet. Put the information in the last column
                                MAINtable_array_temp[0, 14] = "Known Nuissance: " + str(
                                    TopMessagesArray[TopCounter, 13]) \
                                                              + " / In-Service Document: " + str(
                                    TopMessagesArray[TopCounter, 11]) \
                                                              + " / FIM Task: " + str(TopMessagesArray[TopCounter, 10]) \
                                                              + " / Remarks: " + str(TopMessagesArray[TopCounter, 14])

                                # Not need to keep looking
                                TopCounter = TopMessagesArray.shape[0]

                            else:
                                # Not equal, go to next equation
                                MAINtable_array_temp[0, 14] = ""
                                TopCounter += 1
                        # End while

                        if currentRow == 0:
                            MAINtable_array = np.array(MAINtable_array_temp)
                        else:
                            MAINtable_array = np.append(MAINtable_array, MAINtable_array_temp, axis=0)
                        # End if Build MAINtable_array

                        # Move to next Row on Main page for next flag
                        currentRow = currentRow + 1
            TitlesArrayHistory = ["AC SN", "EICAS Message", "MDC Message", "LRU", "ATA", "B1-Equation", "Type",
                                  "Equation Description", "Total Occurences", "Consective Days", "Consecutive FL",
                                  "Intermittent", "Reason(s) for flag", "Priority",
                                  "Known Top Message - Recommended Documents",
                                  "MHIRJ ISE Recommendation", "Additional Comments", "MHIRJ ISE Input"]

            # Converts the Numpy Array to Dataframe to manipulate
            # pd.set_option('display.max_rows', None)
            # Main table
            #global OutputTableHistory
            OutputTableHistory = pd.DataFrame(data=MAINtable_array, columns=TitlesArrayHistory).fillna(" ").sort_values(
                by=["Type", "Priority"])
            OutputTableHistory = OutputTableHistory.merge(AircraftTailPairDF, on="AC SN")  # Tail # added
            OutputTableHistory = OutputTableHistory[
                ["Tail#", "AC SN", "EICAS Message", "MDC Message", "LRU", "ATA", "B1-Equation", "Type",
                 "Equation Description", "Total Occurences", "Consective Days", "Consecutive FL",
                 "Intermittent", "Reason(s) for flag", "Priority", "Known Top Message - Recommended Documents",
                 "MHIRJ ISE Recommendation", "Additional Comments",
                 "MHIRJ ISE Input"]]  # Tail# added to output table which means that column order has to be re ordered
            # OutputTableHistory.to_csv("OutputTableHistory.csv")
            # OutputTableHistory_json = OutputTableHistory.to_json(orient = 'records')
            # return OutputTableHistory_json

            # Get the list of JAM Messages.
            listofJamMessages = list()
            all_jam_messages = connect_to_fetch_all_jam_messages()
            for each_jam_message in all_jam_messages['Jam_Message']:
                listofJamMessages.append(each_jam_message)
            # print(listofJamMessages)

            # HIGHLIGHT function starts here
            OutputTableHistory = OutputTableHistory.assign(
                is_jam=lambda dataframe: dataframe['B1-Equation'].map(
                    lambda c: True if c in listofJamMessages else False)
            )
            # print(OutputTableHistory)
            curr_history = OutputTableHistory
            # OutputTableHistory_json = OutputTableHistory.to_json(orient='records')
            # return OutputTableHistory_json

    # define lists, does the message in the new report exist in the previous or not
    True_list, False_list = create_delta_lists(prev_history, curr_history)
    curr_history.set_index(["AC SN", "B1-Equation"], drop=False, inplace=True)
    prev_history.set_index(["AC SN", "B1-Equation"], drop=False, inplace=True)
    # grab only what exists in the new report from the old report
    prev_history_nums = prev_history.loc[True_list]
    # add the items that only exist in the new report and add them to the old report
    # (this has to be after the True/False Lists are created, bc if done before then everything will be True (list))
    # this was done bc the slicing done for slice_ needs to compare two dataframes with identical indexes
    prev_history_nums = prev_history_nums.append(curr_history.loc[False_list])
    # sort indexes
    curr_history.sort_index(inplace=True)
    prev_history_nums.sort_index(inplace=True)

   
    # delta = highlight_delta(curr_history, True_list)
    # delta_1 = highlight_delta(delta, False_list)

    '''highlights delta of two history reports'''
    # check what in the new report exists in the prev. which of tuple(AC SN, B1) exists in the true list
    
    curr_history['is_light_orange'] = False
    curr_history['is_light_red'] = False
    for each_tuple in curr_history.index.values:
       
        if each_tuple in True_list:
            
            is_color = True
            
        
            currentRow1 = curr_history.loc[curr_history['AC SN'] == each_tuple[0]]
            currentRowwithB1 = currentRow1.loc[currentRow1['B1-Equation'] == each_tuple[1]]
            # if each_tuple[1] == currentRow1['B1-Equation'].any():
            isB1EquationExist = currentRowwithB1.isin(listofJamMessages).any().any()
            
            curr_history.loc[each_tuple , "is_light_orange"] = not isB1EquationExist #reversing bool
             
            # already existed in prev report and is jam
          
            isB1EquationExist2 = currentRowwithB1.isin(listofJamMessages).any().any()
            curr_history.loc[each_tuple , "is_light_red"] =  isB1EquationExist2


            # curr_history = curr_history.assign(
            #     is_light_red=lambda dataframe: dataframe['B1-Equation'].map(
            #         lambda c: True if c in listofJamMessages else False)
            # )
           
        else :
            print('else :')
           
    # print(":: curr_history :: \n",curr_history)
    # delta = curr_history
    curr_history['is_dark_orange'] = False
    curr_history['is_dark_red'] = False
    # iterating for false list
    for each_tuple in curr_history.index.values:
       if each_tuple in False_list:
           is_color = True
         
           currentRow2 = curr_history.loc[curr_history['AC SN'] == each_tuple[0]]
           currentRowwithB2 = currentRow2.loc[currentRow2['B1-Equation'] == each_tuple[1]]
        #    if each_tuple[1] == currentRow2['B1-Equation'].any():
           isB1EquationExist3 = currentRowwithB2.isin(listofJamMessages).any().any()
           curr_history.loc[each_tuple , "is_dark_orange"] = not isB1EquationExist3 #reversing bool
             
           # for each in delta:
              
               # if is_color is True:
                   # didnt exist in prev report and is not jam
                   # delta = delta.assign(
                   #     is_dark_orange=lambda dataframe: dataframe['B1-Equation'].map(
                   #         lambda c: True if c not in listofJamMessages else False)
                   # )
                   # curretRow = curr_history.loc[curr_history['AC SN'] == each_tuple[0]]
                 
 
                   # didnt exist in prev report and is jam
                   # curretRow = curr_history.loc[curr_history['AC SN'] == each_tuple[0]]
           isB1EquationExist4 = currentRowwithB2.isin(listofJamMessages).any().any()
           curr_history.loc[each_tuple , "is_dark_red"] =  isB1EquationExist4
                   # delta = delta.assign(
                   #     is_dark_red=lambda dataframe: dataframe['B1-Equation'].map(
                   #         lambda c: True if c in listofJamMessages else False)
                   # )
            

   

   
   
    # print(":: DELTA :: \n", delta)
    delta_json = curr_history.to_json(orient = 'records')
    return delta_json


    """
    # color what existed previously
    delta = curr_history.style.apply(highlight_delta, axis=1, delta_list=True_list)
    # color what is new
    delta = delta.apply(highlight_delta, axis=1, delta_list=False_list)

    # coloring the counters
    idx = pd.IndexSlice
    # comparing the counters on each report
    # since there are some rows that are added to the prev_history_nums (see above),
    # the values on both dataframes will be equal at the corresponding indexes and wont be highlighted
    # the logic here will only highlight whats strictly greater
    slice_ = idx[
        idx[curr_history["Total Occurrences"] > prev_history_nums["Total Occurrences"]], ["Total Occurrences"]]
    slice_2 = idx[
        idx[curr_history["Consecutive Days"] > prev_history_nums["Consecutive Days"]], ["Consecutive Days"]]
    slice_3 = idx[idx[curr_history["Consecutive FL"] > prev_history_nums["Consecutive FL"]], ["Consecutive FL"]]
    slice_4 = idx[idx[curr_history["Intermittent"] > prev_history_nums["Intermittent"]], ["Intermittent"]]
    delta = delta.set_properties(**{'background-color': '#fabf8f'}, subset=slice_)
    delta = delta.set_properties(**{'background-color': '#fabf8f'}, subset=slice_2)
    delta = delta.set_properties(**{'background-color': '#fabf8f'}, subset=slice_3)
    delta = delta.set_properties(**{'background-color': '#fabf8f'}, subset=slice_4)
"""

	

# blob storage
@app.post("/api/upload_PM_file/")
async def pm_upload_blob(file: UploadFile = File(...)):
    result = run_sample(file)
    return {"result": result}

# Select all data from MDC raw data  
def connect_database_for_mdcRawData(from_date, to_date):
    sql = "SELECT count(*) OVER () as total, c.* From Airline_MDC_Data c WHERE c.DateAndTime BETWEEN '" + from_date + "' AND '" + to_date + "' "

    try:
        conn = pyodbc.connect(driver=db_driver, host=hostname, database=db_name,
                              user=db_username, password=db_password)
                              
        print(sql)
        mdcRaw_df = pd.read_sql(sql, conn)
        #MDCdataDF.columns = column_names
        return mdcRaw_df
    except pyodbc.Error as err:
        print("Couldn't connect to Server")
        print("Error message:- " + str(err))

@app.post("/api/SELECT_MDC_RAW_data/{from_date}/{to_date}")
async def get_mdcRawData(from_date:str, to_date:str):
    mdcRaw_df = connect_database_for_mdcRawData(from_date , to_date)
    total = mdcRaw_df['total'].iloc[0]
    mdcRaw_df = mdcRaw_df.drop(columns=['total'])
    print("total is : ",total)
    mdcRaw_df_json =  mdcRaw_df.to_json(orient='records')
    return  {"total":str(total),"data":mdcRaw_df_json}    

@app.get("/api/getMDCFileUploadStatus")
async def getMDCFileUploadStatus():
    return getFileUploadStatusPercentage() 

def connect_database_mdc_message_input(eq_id):
    sql = "SELECT * from [dbo].[MDCMessagesInputs_CSV_UPLOAD] c WHERE c.Equation_ID='" + eq_id + "' "

    try:
        conn = pyodbc.connect(driver=db_driver, host=hostname, database=db_name,
                              user=db_username, password=db_password)
                              
        print(sql)
        mdcRaw_df = pd.read_sql(sql, conn)
        #MDCdataDF.columns = column_names
        return mdcRaw_df
    except pyodbc.Error as err:
        print("Couldn't connect to Server")
        print("Error message:- " + str(err))

@app.post("/api/list_mdc_messages_input/{eq_id}")
async def get_mdcMessageInput(eq_id:str):
    mdcRaw_df = connect_database_mdc_message_input(eq_id)
    mdcRaw_df_json =  mdcRaw_df.to_json(orient='records')
    return  mdcRaw_df_json