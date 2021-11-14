
import pandas as pd
from Charts.helper import connect_database_for_charts
import numpy as np
import re
import json

def flightlegfill(FLintermittentDF):
    '''Creates an array with the values found in data and fills the rest of the flight leg range with zeros'''
    minFL = int(FLintermittentDF.min().iloc[0])
    maxFL = int(FLintermittentDF.max().iloc[0])
    Flightlegstoplot = pd.Series(range(minFL, maxFL+1))
    Flightlegstoplot = Flightlegstoplot.to_frame(name= "FL No")
    Flightlegstoplot["Values"] = 0
    FLintermittentDF = FLintermittentDF.reset_index()
    
    for i in range(len(Flightlegstoplot)):
            hello = Flightlegstoplot.at[i, "FL No"]
            for j in range(len(FLintermittentDF)):
                there = FLintermittentDF.at[j, "Flight_Leg_No"]
                
                if hello == there:
                    Flightlegstoplot.at[i, "Values"] = FLintermittentDF.at[j, "Intermittent"]
                    
    Flightlegs = Flightlegstoplot["FL No"]
    Valuestoplot = Flightlegstoplot.Values
    return Flightlegs, Valuestoplot

def chart5Report(aircraft_no, equation_id, CurrentFlightPhaseEnabled, fromDate, toDate):
    try:
        MDCdataDF = connect_database_for_charts(aircraft_no, equation_id, CurrentFlightPhaseEnabled, fromDate, toDate)
        if CurrentFlightPhaseEnabled == 1: #Show all, current and history
            FLintermittentDF = MDCdataDF[["Flight_Leg_No","Equation_ID", "Aircraft", "Intermittent"]].copy()
            
        elif CurrentFlightPhaseEnabled == 0: #Only show history
            FLintermittentDF = MDCdataDF[[ "Flight_Leg_No", "Equation_ID", "Aircraft", "Intermittent"]].copy()
            FLintermittentDF = FLintermittentDF.replace(0, np.nan).dropna(axis=0, how='any')
            FLintermittentDF = FLintermittentDF[["Flight_Leg_No","Equation_ID", "Aircraft", "Intermittent"]].copy()

        removeParanthesisRegex = r"[()]"
        equation_ids = re.sub(removeParanthesisRegex, '', equation_id)
        unavailableEquationIds = []
        chart5Result = {}
        for equation in equation_ids.split(','):
            equation = re.sub(r"['']",'',equation)
            try:
                flintDF = FLintermittentDF.set_index(["Aircraft", "Equation_ID"]).sort_index().loc[('AC'+str(aircraft_no), equation)]
                xaxis, yaxis = flightlegfill(flintDF)
                valueObj = {}
                for i in range(len(xaxis)):
                    valueObj[str(xaxis[i])] = str(yaxis[i])
                
                chart5Result[equation] = valueObj
            except Exception as e:
                unavailableEquationIds.append(equation)
                print(e)

        if len(unavailableEquationIds) != 0 and not chart5Result:
            chart5Result = {'UnavailableEquationIds': unavailableEquationIds}
        
        return json.dumps(chart5Result)

    except:
        return json.dumps({'Error': 'Something went wrong!!'})
            