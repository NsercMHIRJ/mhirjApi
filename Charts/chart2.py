from Charts.helper import connect_database_for_chart2
import pandas as pd
import json
import numpy as np

def chart_two(top_values, ata, fromDate,toDate):
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
        

