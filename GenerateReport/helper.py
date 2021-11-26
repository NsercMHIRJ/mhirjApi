from app import App
import pyodbc
import pandas as pd
import datetime

conn = pyodbc.connect(driver=App().db_driver, host=App().hostname, database=App().db_name,
                              user=App().db_username, password=App().db_password)
def isValidParams(occurences: int, legs: int, intermittent: int, consecutiveDays: int, ata: str, exclude_EqID:str, airline_operator: str, include_current_message: int, fromDate: str , toDate: str):
    return True


def connect_to_fetch_all_ata(from_dt, to_dt):
    all_ata_query = "SELECT DISTINCT ATA_Main from Airline_MDC_Data WHERE DateAndTime BETWEEN '" + from_dt + "' AND '" + to_dt + "'"
    try:
        all_ata_df = pd.read_sql(all_ata_query, conn)

        return all_ata_df
    except pyodbc.Error as err:
        print("Couldn't connect to Server")
        print("Error message:- " + str(err))

def connect_to_fetch_all_eqids(from_dt, to_dt):
    all_ata_query = "SELECT DISTINCT Equation_ID from Airline_MDC_Data WHERE DateAndTime BETWEEN '" + from_dt + "' AND '" + to_dt + "'"
    try:
        all_eqid_df = pd.read_sql(all_ata_query, conn)

        return all_eqid_df
    except pyodbc.Error as err:
        print("Couldn't connect to Server")
        print("Error message:- " + str(err))


def connect_database_MDCdata(ata, excl_eqid, airline_operator, include_current_message, from_dt, to_dt):
    MDCdataDF = ""
    airline_id = ""
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

    from_dt = from_dt + " 00:00:00"
    to_dt = to_dt + " 23:59:59"
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
        MDCdataDF = pd.read_sql(sql, conn)
        MDCdataDF.columns = column_names
        return MDCdataDF
    except pyodbc.Error as err:
        print("Couldn't connect to Server")
        print("Error message:- " + str(err))


def connect_database_MDCmessagesInputs():
    global MDCMessagesDF
    sql = "SELECT * FROM MDCMessagesInputs_CSV_UPLOAD" #MDCMessagesInputs_CSV_UPLOAD

    try:
        MDCMessagesDF = pd.read_sql(sql, conn)
        print(MDCMessagesDF.columns)
        return MDCMessagesDF
    except pyodbc.Error as err:
        print("Couldn't connect to Server")
        print("Error message:- " + err)

def connect_database_TopMessagesSheet():
    global TopMessagesDF
    sql = "SELECT * FROM TopMessagesSheet"

    try:
        TopMessagesDF = pd.read_sql(sql, conn)
        return TopMessagesDF
    except pyodbc.Error as err:
        print("Couldn't connect to Server")
        print("Error message:- " + err)

def connect_to_fetch_all_jam_messages():
    jam_messages_query = "SELECT * from JamMessagesList"
    try:
        jam_messages_df = pd.read_sql(jam_messages_query, conn)

        return jam_messages_df
    except pyodbc.Error as err:
        print("Couldn't connect to Server")
        print("Error message:- " + str(err))

def highlightJams(Outputtable, listofJamMessages, column= "B1-Equation"):
    ''' highlights the rows where a Jam message was flagged on the report. 
    Note: This color will also print on to_excel 
    Outputtable: Either Historyreport or dailyreport
    listofmessages: List of jam messages, but could also add other types of messages to flag
    Column: column to check
    
    you can use this link to change the color to highlight
    https://matplotlib.org/stable/gallery/color/named_colors.html'''
    
    Outputtable = Outputtable.assign(
        is_jam=lambda dataframe: dataframe[column].map(lambda c: True if c in listofJamMessages else False)
    )
    return Outputtable

#function used later on to display the messages in the same Flight Leg as the Jam flags raised
def flagsinreport(OutputTable, Aircraft, listofJamMessages, MDCdataDF):
    ''' display the messages in the same Flight Leg as the Jam flags
        OutputTable: Either HistoryReport or Dailyreport
        Listofmessages: Jam Messages
        Return: Dataframe indexed with AC SN and Flight Leg #
    '''
    datatofilter = MDCdataDF.copy(deep= True)
    
    isin = OutputTable["B1-Equation"].isin(listofJamMessages)
    filter1 = OutputTable[isin][["AC SN", "B1-Equation"]]
    listoftuplesACID = list(zip(filter1["AC SN"], filter1["B1-Equation"]))
    
    datatofilter2 = datatofilter.set_index(["Aircraft", "Equation ID"]).sort_index().loc[pd.IndexSlice[listoftuplesACID], :].reset_index()
    listoftuplesACFL = list(zip(datatofilter2["Aircraft"], datatofilter2["Flight Leg No"]))
    
    datatofilter3 = datatofilter.set_index(["Aircraft", "Flight Leg No"]).sort_index()
    FinalDF = datatofilter3.loc[pd.IndexSlice[listoftuplesACFL], :]
    return FinalDF.loc[Aircraft]


def LongestConseq(unique_arr, days_legs):
    '''
    Finds the maximum consecutive number of days or legs in the passed array. 
    Passed array has to be sorted in descending order for the algorithm to work

    Input:
    unique_arr: A sorted array/list (descending) in datetime format to operate on
    days_Legs: "days" or "legs" user choice 

    Output:
    ans: Maximum number of consecutive days or legs in the array
    '''

    ans = 0
    count = 0

    if days_legs == "days":
        # Find the maximum length
        # by traversing the array
        for i in range(len(unique_arr)):
            if (i > 0 and unique_arr[i] == unique_arr[i - 1] - datetime.timedelta(1)):
                count += 1

            # Reset the count
            else:
                count = 1

            # Update the maximum
            ans = max(ans, count)

    elif days_legs == "legs":
        # Find the maximum length
        # by traversing the array
        for i in range(len(unique_arr)):
            if (i > 0 and int(unique_arr[i]) == int(unique_arr[i - 1] -1)):
                count += 1

            # Reset the count
            else:
                count = 1

            # Update the maximum
            ans = max(ans, count)
    else:
        raise NameError("Only 'days' or 'legs' can be passed into days_legs arg")

    return ans    

def check_2in5(dates):
    '''Returns true if the list of days passed has 2 entries within 5 days'''

    # Iterate through days
    for i in range(len(dates)):

        if i == 0:pass

        # If day is less than 5 days before the previous day return true
        elif dates[i] > dates[i-1] - datetime.timedelta(5): return True

    return False