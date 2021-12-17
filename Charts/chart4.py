from GenerateReport.daily import dailyReport
from GenerateReport.history import historyReport
import pandas as pd
import json
import numpy as np

def chart4Report(occurences, legs, intermittent, consecutiveDays, ata, exclude_EqID, airline_operator, include_current_message, fromDate , toDate, topCount, analysisType):
    outputjson = ""
    if analysisType.lower() == "history":
        outputjson = historyReport(occurences, legs, intermittent, consecutiveDays, ata, exclude_EqID, airline_operator, include_current_message, fromDate , toDate)
        outputjson = outputjson.to_json(orient='records')
    else:
        outputjson = dailyReport(occurences, legs, intermittent, consecutiveDays, ata, exclude_EqID, airline_operator, include_current_message, fromDate , toDate)
    
    if outputjson != "":
        data = json.loads(outputjson)
        OutputTableHistory = pd.json_normalize(data)
        print("history: ", OutputTableHistory.columns)
        Dataforchart = OutputTableHistory["ATA"]
        labels = Dataforchart.value_counts().index
        countsofATAs = Dataforchart.value_counts().sort_values().tail(20)

        #Image settings
        spacing = np.arange(len(countsofATAs))
        return countsofATAs.to_json()
    else:
        return json.dumps({"Message", "No data found!!!"})



