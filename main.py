# Importing libraries to the project
#!/usr/bin/bash
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


app = FastAPI()
# origins = [

#     "https://GenerateReport/{analysisType}/{occurences}/{legs}/{intermittent}/{consecutiveDays}/{airlineOperator}/{ata}/{messages}/{fromDate}/{toDate}",

#     "http://localhost",
#     "http://localhost:8000",
    
# ]
#origins=["*"]
"""Create a list of allowed origins ( as strings)
"""
#app.add_middleware(
#    CORSMiddleware,
#    allow_origins=["*"],
#    allow_credentials=True,
#    allow_methods=["*"],
#    allow_headers=["*"],
#)

#####

OutputTableHistory2 = pd.DataFrame()
MDCeqns_arrayforgraphing = pd.DataFrame()

hostname = os.environ.get('hostname', 'mhirjserver.database.windows.net')
db_server_port = urllib.parse.quote_plus(str(os.environ.get('db_server_port', '8080')))
db_name = os.environ.get('db_name', 'MHIRJ')
db_username = urllib.parse.quote_plus(str(os.environ.get('db_username', 'mhirj-admin')))
db_password = urllib.parse.quote_plus(str(os.environ.get('db_password', 'KaranCool123')))
ssl_mode = urllib.parse.quote_plus(str(os.environ.get('ssl_mode','prefer')))
db_driver = "ODBC Driver 17 for SQL Server"

CurrentFlightPhaseEnabled = 0
MDCdataDF = pd.DataFrame()

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
    #ata_list = tuple(ata)
    #excl_eqid_list = tuple(excl_eqid)
    #for each_ata in ata:
    #    print(each_ata)
    if airline_operator.upper() == "SKW":
        print(airline_operator.upper())
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


    if include_current_message == 0:    # If we do not include current message = exclude null flight phase and null intermittents
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

    column_names = ["Aircraft", "Tail", "Flight Leg No",
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
        # add column names from csv file into dataframe
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

# @app.post("/MDCRawData/{ATAMain_list}/{exclude_EqID_list}/{airline_operator}/{include_current_message}/{fromDate}/{toDate}")
# async def get_MDCRawData(ATAMain_list:str, exclude_EqID_list:str, airline_operator:str, include_current_message:int, fromDate: str , toDate: str):
#
#     c = connect_database_MDCdata(ATAMain_list, exclude_EqID_list, airline_operator, include_current_message, fromDate, toDate)
#     #print(c['DateAndTime'].astype('datetime64[s]'))
#     #c['DateAndTime'] = c['DateAndTime'].astype('datetime64[s]')
#     c['DateAndTime'] = c['DateAndTime'].str.strip(':00.0000000')
#     MDCdataDF_json = c.to_json(orient='records')
#     return MDCdataDF_json
#    return jsonify(MDCdataDF_json1)


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


# @app.post("/MDCRawData/{ATAMain_list}/{exclude_EqID_list}/{airline_operator}/{include_current_message}/{fromDate}/{toDate}")
# async def get_MDCRawData(ATAMain_list:str, exclude_EqID_list:str, airline_operator:str, include_current_message:int, fromDate: str , toDate: str):
#
#     c = connect_database_MDCdata(ATAMain_list, exclude_EqID_list, airline_operator, include_current_message, fromDate, toDate)
#     #print(c['DateAndTime'].astype('datetime64[s]'))
#     #c['DateAndTime'] = c['DateAndTime'].astype('datetime64[s]')
#     print(type(c['DateAndTime']))
#     c['DateAndTime'] = c['DateAndTime'].to_string().strip(':00.0000000')
#     MDCdataDF_json = c.to_json(orient='records')
#     return MDCdataDF_json





#for Daily Report: value of consecutiveDays = 0 in URL -> for reference!!       ('32','22')/('B1-007553', 'B1-246748')/skw/1/2020-11-11/2020-11-12
@app.post("/api/GenerateReport/{analysisType}/{occurences}/{legs}/{intermittent}/{consecutiveDays}/{ata}/{exclude_EqID}/{airline_operator}/{include_current_message}/{fromDate}/{toDate}")
async def generateReport(analysisType: str, occurences: int, legs: int, intermittent: int, consecutiveDays: int, ata: str, exclude_EqID:str, airline_operator: str, include_current_message: int, fromDate: str , toDate: str):
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
    MDCdataDF["Aircraft"] = MDCdataDF["Aircraft"].str.replace('AC', '')
    MDCdataDF.fillna(value=" ", inplace=True)  # replacing all REMAINING null values to a blank string

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
    MaxAllowedIntermittent = intermittent  # flag for intermittent values ->IM
    MaxAllowedConsecDays = consecutiveDays
    #Bcode = equationID
    newreport = True    #set a counter variable to bring back it to false

    if(analysisType == "daily"):

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

        OutputTableDaily_json = OutputTableDaily.to_json(orient = 'records')
        #OutputTableDaily.to_csv("OutputTableDaily.csv")

        return OutputTableDaily_json

    elif(analysisType == "history"):
        #global UniqueSerialNumArray

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

        # Converts the Numpy Array to Dataframe to manipulate
        # pd.set_option('display.max_rows', None)
        # Main table
        global OutputTableHistory
        OutputTableHistory = pd.DataFrame(data=MAINtable_array, columns=TitlesArrayHistory).fillna(" ").sort_values(
            by=["Type", "Priority"])
        #OutputTableHistory.to_csv("OutputTableHistory.csv")
        OutputTableHistory_json = OutputTableHistory.to_json(orient = 'records')
        return OutputTableHistory_json



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
@app.post("/api/GenerateReport/{analysisType}/{occurences}/{legs}/{intermittent}/{consecutiveDays}/{ata}/{exclude_EqID}/{airline_operator}/{include_current_message}/{fromDate}/{toDate}/{list_of_tuples_acsn_bcode}")
async def generateFlagReport(analysisType: str, occurences: int, legs: int, intermittent: int, consecutiveDays: int, ata: str, exclude_EqID:str, airline_operator: str, include_current_message: int, fromDate: str , toDate: str, list_of_tuples_acsn_bcode):
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
    MDCdataDF["Aircraft"] = MDCdataDF["Aircraft"].str.replace('AC', '')
    MDCdataDF.fillna(value=" ", inplace=True)  # replacing all REMAINING null values to a blank string

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

    if (analysisType.upper() == "DAILY"):

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
        DailyReport = OutputTableDaily


    if (analysisType.upper() == "HISTORY"):
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

        # Converts the Numpy Array to Dataframe to manipulate
        # pd.set_option('display.max_rows', None)
        # Main table
        #global OutputTableHistory
        OutputTableHistory = pd.DataFrame(data=MAINtable_array, columns=TitlesArrayHistory).fillna(" ").sort_values(
            by=["Type", "Priority"])
        HistoryReport = OutputTableHistory

        ##### create a flags report #####
        DatesDF = MDCdataDF[["DateAndTime", "Equation ID", "Aircraft", "Flight Phase"]]

        if (analysisType.upper() == "DAILY"):
            Report = DailyReport
        elif (analysisType.upper() == "HISTORY"):
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



# Plots
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

## Chart 2
def connect_database_for_chart2(n, ata, from_dt, to_dt):
    if len(ata) == 2:
        sql = "SELECT DISTINCT TOP "+str(n)+" COUNT(ATA_Main) AS "'ataOcc'", aircraft FROM Airline_MDC_Data where ATA_Main='"+ata+"' AND DateAndTime BETWEEN '"+from_dt+"' AND '"+to_dt+"' GROUP BY ATA_Main, Aircraft ORDER BY COUNT(ATA_Main) DESC"
    elif len(ata) == 5:
        sql = "SELECT DISTINCT TOP "+str(n)+" COUNT(ATA) AS "'ataOcc'", aircraft FROM Airline_MDC_Data where ATA='"+ata+"' AND DateAndTime BETWEEN '"+from_dt+"' AND '"+to_dt+"' GROUP BY ATA, Aircraft ORDER BY COUNT(ATA) DESC"

    print(sql)
    try:
        conn = pyodbc.connect(driver=db_driver, host=hostname, database=db_name,
                              user=db_username, password=db_password)
        chart2_sql_df = pd.read_sql(sql, conn)
        # MDCdataDF.columns = column_names
        conn.close()
        return chart2_sql_df
    except pyodbc.Error as err:
        print("Couldn't connect to Server")
        print("Error message:- " + str(err))

@app.post("/api/chart_two/{top_values}/{ata}/{fromDate}/{toDate}")
async def get_ChartwoData(top_values:int, ata:str, fromDate: str , toDate: str):
    chart2_sql_df = connect_database_for_chart2(top_values, ata, fromDate, toDate)
    chart2_sql_df_json = chart2_sql_df.to_json(orient='records')
    return chart2_sql_df_json


## Chart 3
def connect_database_for_chart3(aircraft_no, equation_id, is_flight_phase_enabled, from_dt, to_dt):
    if is_flight_phase_enabled == 0: # Flight phase is NOT enabled
        sql = "SELECT COUNT(*) AS OccurencesPerDay, cast(DateAndTime as DATE) AS Dates from Airline_MDC_Data WHERE Equation_ID='"+equation_id+"' AND aircraftno = '"+str(aircraft_no)+"' AND Flight_Phase IS NOT NULL AND DateAndTime BETWEEN '"+from_dt+"' AND '"+to_dt+"' GROUP BY cast(DateAndTime as DATE)"
    elif is_flight_phase_enabled == 1:
        sql = "SELECT COUNT(*) AS OccurencesPerDay, cast(DateAndTime as DATE) AS Dates from Airline_MDC_Data WHERE Equation_ID='"+equation_id+"' AND aircraftno = '"+str(aircraft_no)+"' AND Flight_Phase IS NULL AND DateAndTime BETWEEN '"+from_dt+"' AND '"+to_dt+"' GROUP BY cast(DateAndTime as DATE)"
    print(sql)
    try:
        conn = pyodbc.connect(driver=db_driver, host=hostname, database=db_name,
                              user=db_username, password=db_password)
        chart3_sql_df = pd.read_sql(sql, conn)
        # MDCdataDF.columns = column_names
        conn.close()
        return chart3_sql_df
    except pyodbc.Error as err:
        print("Couldn't connect to Server")
        print("Error message:- " + str(err))

@app.post("/api/chart_three/{aircraft_no}/{equation_id}/{is_flight_phase_enabled}/{fromDate}/{toDate}")
async def get_CharThreeData(aircraft_no:int, equation_id:str, is_flight_phase_enabled:int, fromDate: str , toDate: str):
    chart3_sql_df = connect_database_for_chart3(aircraft_no, equation_id, is_flight_phase_enabled, fromDate, toDate)
    chart3_sql_df_json = chart3_sql_df.to_json(orient='records')
    return chart3_sql_df_json


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
    chart5_sql_df = connect_database_for_chart5(aircraft_no, equation_id, is_flight_phase_enabled, fromDate, toDate)
    chart5_sql_df.replace({'>':0}, inplace=True) #To replace the error value '>' with 0 in Intermittent Column.
    chart5_sql_df_json = chart5_sql_df.to_json(orient='records')
    return chart5_sql_df_json



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
    sql = "SELECT * FROM Airline_MDC_Data WHERE DateAndTime BETWEEN '11-5-2020' AND '11-12-2020'"
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
    Topvalues2 = 10
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
    print(TransposedMessageCountbyAircraftATAfinalPLOT.columns)
    chart_b_df_json = TransposedMessageCountbyAircraftATAfinalPLOT.to_json(orient='index')
    return chart_b_df_json


def connect_db_MDCdata_chartb(from_dt, to_dt):
    sql = "SELECT * FROM Airline_MDC_Data WHERE DateAndTime BETWEEN '" + from_dt + "' AND '" + to_dt + "'"
    column_names = ["Aircraft", "Tail", "Flight Leg No",
                    "ATA Main", "ATA Sub", "ATA", "ATA Description", "LRU",
                    "DateAndTime", "MDC Message", "Status", "Flight Phase", "Type",
                    "Intermittent", "Equation ID", "Source", "Diagnostic Data",
                    "Data Used to Determine Msg", "ID", "Flight", "airline_id", "aircraftno"]
    print(sql)
    try:
        conn = pyodbc.connect(driver='{SQL Server}', host=hostname, database=db_name,
                              user=db_username, password=db_password)
        MDCdataDF_chartb = pd.read_sql(sql, conn)
        MDCdataDF_chartb.columns = column_names
        conn.close()
        return MDCdataDF_chartb
    except pyodbc.Error as err:
        print("Couldn't connect to Server")
        print("Error message:- " + str(err))

# for reference -> http://localhost:8000/Landing_Chart_B/15/11-11-2020/11-17-2020
@app.post("/api/Landing_Chart_B/{top_n}/{from_dt}/{to_dt}")
async def get_Chart_B(top_n: int,from_dt: str, to_dt: str):
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
		print(es)


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
    equation_id = str(tuple(equation_id.replace(")", "").replace("(", "").replace("'", "").split(",")))
    if "ALL" not in ata:
        ata = str(tuple(ata.replace(")", "").replace("(", "").replace("'", "").split(",")))
    sql = ''

    equation_id = str(tuple(equation_id.replace(")","").replace("(","").replace("'","").split(",")))
    ata = str(tuple(ata.replace(")","").replace("(","").replace("'","").split(",")))
    sql =""

    sql += "SELECT distinct p_id ,Operator ,Model ,Type ,Serial_No ,N_No ,Date, [Failure Flag] ,[Maint Trans] ,[Maintenance Cancellations] ,[Maintenance Delays] ,Inspection,CampType ,MRB ,Discrepancy ,[Corrective Action] ,[AC Total Hours] ,[AC Total Cycles],[Squawk Source] ,ATA ,Station ,ATA_SUB ,ATA_Main FROM dbo.sample_corelation WHERE CONVERT(date,Date) between '" + from_dt + "'  AND '" + to_dt + "'"


    if equation_id:
        sql += "  AND EQ_ID NOT IN " + equation_id
    if "ALL" not in ata :
        sql += "  AND mdc_ata_Main IN " + ata
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
@app.post("/api/corelation/{fromDate}/{toDate}/{equation_id}/{ata}")
async def get_CorelationData(fromDate: str , toDate: str, equation_id:str, ata:str):
    corelation_df = connect_database_for_corelation(fromDate, toDate, equation_id, ata)
    corelation_df_json = corelation_df.to_json(orient='records')
    return corelation_df_json


def connect_database_for_corelation_pid(p_id):
    
    sql = """SELECT [mdc_ID], [EQ_ID], [aircraftno], [ATA_Description], [LRU], [DateAndTime], [MDC_Date], 
	[MDC_MESSAGE], [EQ_DESCRIPTION], [CAS], [LRU_CODE], [LRU_NAME], [FAULT_LOGGED], [MDC_ATA], 
	[mdc_ata_main], [mdc_ata_sub], [Status], [mdc_type]
    FROM [dbo].[sample_corelation]
    WHERE p_id = (%s)
    ORDER BY MDC_Date""" %(p_id)

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

def connect_database_for_corelation_pid(p_id):
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
