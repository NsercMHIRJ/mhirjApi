
from Charts.helper import connect_database_for_charts
import pandas as pd
import numpy as np
import re
import json

def chart3Report(aircraft_no, equation_id, CurrentFlightPhaseEnabled, fromDate, toDate):
    print(aircraft_no, equation_id, CurrentFlightPhaseEnabled, fromDate, toDate)
    MDCdataDF = connect_database_for_charts(aircraft_no, equation_id, CurrentFlightPhaseEnabled, fromDate, toDate)
    MDCdataDF["DateAndTime"] = pd.to_datetime(MDCdataDF["DateAndTime"])

    if CurrentFlightPhaseEnabled == 1: #Show all, current and history
        DateNmessageDF = MDCdataDF[["DateAndTime","Equation_ID", "Aircraft"]].copy()
        
    elif CurrentFlightPhaseEnabled == 0: #Only show history
        DateNmessageDF = MDCdataDF[["DateAndTime","Equation_ID", "Aircraft", "Flight_Phase"]].copy()
        DateNmessageDF = DateNmessageDF.replace(False, np.nan).dropna(axis=0, how='any')
        DateNmessageDF = DateNmessageDF[["DateAndTime","Equation_ID", "Aircraft"]].copy()
        
    pd.to_datetime(DateNmessageDF["DateAndTime"])
    counts = pd.DataFrame(data= DateNmessageDF.groupby(['Aircraft', "Equation_ID", "DateAndTime"]).agg(len), columns= ["Counts"])

    removeParanthesisRegex = r"[()]"
    equation_ids = re.sub(removeParanthesisRegex, '', equation_id)
    valuesfoundinMDCdata = ''
    unavailableEquationIds = []
    for equation in equation_ids.split(','):
        equation = re.sub(r"[' ']",'',equation)
        try:
            valuesfoundinMDCdata += counts.loc[('AC'+str(aircraft_no), equation)].resample('D')["Counts"].sum().to_json()
        except:
            unavailableEquationIds.append(equation)

    if len(unavailableEquationIds) != 0 and not valuesfoundinMDCdata:
        valuesfoundinMDCdata = json.dumps({'UnavailableEquationIds': unavailableEquationIds})

    print(valuesfoundinMDCdata)
    return valuesfoundinMDCdata



def datefill(datesindata, valuesindata, NumDays, enddate):
    print('came to matplot')
    Datatoplot = pd.date_range(end= enddate, periods= NumDays, freq= "D")
    Datatoplot = Datatoplot.to_frame(index= False, name = "Date")
    Datatoplot["Values"] = 0
    Datesindata = datesindata.to_frame(index= False, name = "Date")
    
    for i in range(NumDays):
            hi = Datatoplot.at[i, "Date"]
            for j in range(len(Datesindata)):
                bye = Datesindata.at[j, "Date"]
                
                if hi == bye:
                    Datatoplot.at[i, "Values"] = valuesindata[j]
                    
    Matplotlibdates = matplotlib.dates.date2num(Datatoplot.Date)
    Valuestoplot = Datatoplot.Values
    return Matplotlibdates, Valuestoplot