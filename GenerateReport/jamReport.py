from GenerateReport.helper import LongestConseq, check_2in5, connect_database_MDCdata, connect_database_MDCmessagesInputs, connect_database_TopMessagesSheet, connect_to_fetch_all_jam_messages, highlightJams, isValidParams
import numpy as np
import pandas as pd
from GenerateReport.history import historyReport

def mdcDF(MaxAllowedOccurrences: int, MaxAllowedConsecLegs: int, MaxAllowedIntermittent: int, MaxAllowedConsecDays: int, ata: str, exclude_EqID:str, airline_operator: str, include_current_message: int, fromDate: str , toDate: str):
    MDCdataDF = connect_database_MDCdata(ata, exclude_EqID, airline_operator, include_current_message, fromDate, toDate)
    MDCdataDF["Aircraft"] = MDCdataDF["Aircraft"].str.replace('AC', '')
    MDCdataDF.fillna(value= " ", inplace= True) # replacing all REMAINING null values to a blank string
    MDCdataDF.sort_values(by= "DateAndTime", ascending= False, inplace= True, ignore_index= True)
    return MDCdataDF


 
def jamReport(OutputTableHistory, ACSN_chosen,MDCdataDF):

   listofJamMessages = list()
   all_jam_messages = connect_to_fetch_all_jam_messages()
   for each_jam_message in all_jam_messages['Jam_Message']:
       listofJamMessages.append(each_jam_message)
 
   datatofilter = MDCdataDF.copy(deep=True)
   isin = OutputTableHistory["B1-Equation"].isin(listofJamMessages)
 
   filter1 = OutputTableHistory[isin][["AC SN", "B1-Equation"]]
   listoftuplesACID = list(zip(filter1["AC SN"], filter1["B1-Equation"]))
 
   datatofilter2 = datatofilter.set_index(["Aircraft", "Equation ID"]).sort_index().loc[
                           pd.IndexSlice[listoftuplesACID], :].reset_index()
   listoftuplesACFL = list(zip(datatofilter2["Aircraft"], datatofilter2["Flight Leg No"]))
   datatofilter3 = datatofilter.set_index(["Aircraft", "Flight Leg No"]).sort_index()
   FinalDF = datatofilter3.loc[pd.IndexSlice[listoftuplesACFL], :]
   FinalDF_history_json = (FinalDF.loc[str(ACSN_chosen)])

   return FinalDF_history_json
