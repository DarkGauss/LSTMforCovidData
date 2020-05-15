# Matthew Holman            5-3-2020
# CS504
#
# Data joining in pandas for Python for Machine Learning (PML) final project
#
# NOTE: You must make a directory folder called 'output' in the same folder that dataJoin.py is in:
#
# ./output/
# ./dataJoin.py
#
# NOTE: The input file names must appear *exactly* as I have them here (note that spaces in the original name could be removed):
#
# StateCasesData.csv
# StateMobilityData.csv
# StateTestingData.csv
# StateDensityData.csv

import pandas as pd
import numpy as np
import re

# (some) Relevant columns in CSV files
# state, county, date

# Step 1: Read in all the data
# Step 2: Join 'cases' and 'mobil' files on state, county, and date creating 'master' file
# Step 3: Join StateTestingData on state to 'master' file 
# Step 4: Join StatePopulationDensity on state to 'master' file
# Step 5: Strip out all state data from 'master' file to individual files which only include "Confirmed" data, sorted by date
# Step 6: Find the state with the smallest number of entries
# Step 7: Make all other states have that same number of entries from step 6

# Step 1: Read in all the data
cases_csv = pd.read_csv("StateCasesData.csv") #, dtype=str
mobil_csv = pd.read_csv("StateMobilityData.csv")
testi_csv = pd.read_csv("StateTestingData.csv")
densi_csv = pd.read_csv("StateDensityData.csv")
print("## NOTE: Data Has Been Read In...")

# Step 2: Join 'cases' and 'mobil' files on state, county, and date creating 'master' file
master = cases_csv.merge(mobil_csv, how="inner", left_on=["state", "county", "date"], right_on=["state", "county", "date"])
print("## NOTE: cases/ mobil Merge Complete...")

# Step 3: Join StateTestingData on state to 'master' file
master = master.merge(testi_csv, how="inner", left_on=["state", "date"], right_on=["state", "date"])
print("## NOTE: master/ testi_csv Merge Complete...")

# Step 4: Join StatePopulationDensity on state to 'master' file
master = master.merge(densi_csv, how="inner", left_on=["state"], right_on=["state"])
print("## NOTE: master/ densi Merge Complete...")

# Step 5: Strip out all state data from 'master' file to individual files which only include "Confirmed" data, sorted by date
droplist = ["inIcuCurrently","onVentilatorCurrently","inIcuCumulative",\
            "onVentilatorCurrently","onVentilatorCumulative","hash",\
            "dateChecked","Data_Source","Lat","Long","Prep_Flow_Runtime",\
            "country_region_code","country_region","FIPS","iso2","iso3","fips",\
            "total","totalTestResults","posNeg","hospitalizedIncrease","totalTestResultsIncrease",\
            "positiveIncrease","negativeIncrease","People_Hospitalized_Cumulative_Count",\
            "hospitalizedCurrently","pending","hospitalized","Pop","Combined_Key","Case_Type","county","Country_Region","state",]
grouped = master.groupby("state")
regionDFGroup = []
for name, region in grouped:
    region = region[region.Case_Type == "Confirmed"]
    region = region.drop(columns=droplist)
    region = region.sort_values(by=["date"])
    region = region.fillna(0)
    #region.to_csv("output/" + str(name) + ".csv", index=False)
    df = pd.DataFrame(columns=["People_Total_Tested_Count","Cases","Difference","date",\
        	"Population_Count","retail_and_recreation_percent_change_from_baseline","grocery_and_pharmacy_percent_change_from_baseline",\
            "parks_percent_change_from_baseline","transit_stations_percent_change_from_baseline","workplaces_percent_change_from_baseline",\
            "residential_percent_change_from_baseline",	"positive","negative","hospitalizedCumulative","recovered","death","deathIncrease","Density","LandArea"])
    dateGrouped = region.groupby("date")
    nameAndDF = []
    for dateName, dateRegion in dateGrouped: 
        df = df.append({#"Case_Type" : dateRegion["Case_Type"].iloc[0],
                    "People_Total_Tested_Count" : dateRegion["People_Total_Tested_Count"].sum(),
                    "Cases" : dateRegion["Cases"].sum(),
                    "Difference" : dateRegion["Difference"].sum(),
                    "date" : dateRegion["date"].iloc[0],
                    #"Country_Region" : dateRegion["Country_Region"].iloc[0],
                    #"state" : dateRegion["state"].iloc[0],
                    "Population_Count" : dateRegion["Population_Count"].sum(),
                    "retail_and_recreation_percent_change_from_baseline" : dateRegion["retail_and_recreation_percent_change_from_baseline"].mean(),
                    "grocery_and_pharmacy_percent_change_from_baseline" :  dateRegion["grocery_and_pharmacy_percent_change_from_baseline"].mean(),
                    "parks_percent_change_from_baseline" : dateRegion["parks_percent_change_from_baseline"].mean(),
                    "transit_stations_percent_change_from_baseline" : dateRegion["transit_stations_percent_change_from_baseline"].mean(),
                    "workplaces_percent_change_from_baseline" : dateRegion["workplaces_percent_change_from_baseline"].mean(),
                    "residential_percent_change_from_baseline" : dateRegion["residential_percent_change_from_baseline"].mean(),	
                    "positive" : dateRegion["positive"].iloc[0],
                    "negative" : dateRegion["negative"].iloc[0],
                    "hospitalizedCumulative" : dateRegion["hospitalizedCumulative"].iloc[0],
                    "recovered" : dateRegion["recovered"].iloc[0],
                    "death" : dateRegion["death"].iloc[0],
                    "deathIncrease" : dateRegion["deathIncrease"].iloc[0],
                    "Density" : dateRegion["Density"].iloc[0],
                    "LandArea" : dateRegion["LandArea"].iloc[0]
                    }, ignore_index=True)
    #print(df["Cases"])
    df = df[df.Cases != 0]
    df.reset_index(drop=True)
    nameAndDF.append(name)
    nameAndDF.append(df)
    regionDFGroup.append(nameAndDF)
    print("## NOTE: " + str(name) + " Has Been Initially Processed...")

# Step 6: Find the state with the smallest number of entries
smallestName = ""
smallestLength = 9999
for region in regionDFGroup:
    totalRows = len(region[1].index)
    if totalRows < smallestLength:
        smallestLength = totalRows
        smallestName = region[0]
print("## NOTE: The Smallest Entry Has Been Found. It is: " + str(region[0]) + " With Length: " + str(smallestLength) + "...")

# Step 7: Make all other states have that same number of entries from step 6
for region in regionDFGroup:
    #finalRegion = region[1][0:smallestLength]
    #finalRegion.to_csv("output/" + str(region[0]) + ".csv", index=False)
    region[1].to_csv("output/" + str(region[0]) + ".csv", index=False)
    print("## NOTE: " + str(region[0]) + " Has Been Written...")
print("## NOTE: All Files Written...")