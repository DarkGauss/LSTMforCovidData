# Matthew Holman            5-3-2020
# CS504
#
# Data changing for the final project
#
# NOTE: You must make a directory folder called 'output' in the same folder that dataChanger.py is in:
#
# ./output/
# ./dataJoin.py

import pandas as pd
import numpy as np
import re

states = {
        "AK": "Alaska",
        "AL": "Alabama",
        "AR": "Arkansas",
        "AS": "American Samoa",
        "AZ": "Arizona",
        "CA": "California",
        "CO": "Colorado",
        "CT": "Connecticut",
        "DC": "District of Columbia",
        "DE": "Delaware",
        "FL": "Florida",
        "GA": "Georgia",
        "GU": "Guam",
        "HI": "Hawaii",
        "IA": "Iowa",
        "ID": "Idaho",
        "IL": "Illinois",
        "IN": "Indiana",
        "KS": "Kansas",
        "KY": "Kentucky",
        "LA": "Louisiana",
        "MA": "Massachusetts",
        "MD": "Maryland",
        "ME": "Maine",
        "MI": "Michigan",
        "MN": "Minnesota",
        "MO": "Missouri",
        "MP": "Northern Mariana Islands",
        "MS": "Mississippi",
        "MT": "Montana",
        "NA": "National",
        "NC": "North Carolina",
        "ND": "North Dakota",
        "NE": "Nebraska",
        "NH": "New Hampshire",
        "NJ": "New Jersey",
        "NM": "New Mexico",
        "NV": "Nevada",
        "NY": "New York",
        "OH": "Ohio",
        "OK": "Oklahoma",
        "OR": "Oregon",
        "PA": "Pennsylvania",
        "PR": "Puerto Rico",
        "RI": "Rhode Island",
        "SC": "South Carolina",
        "SD": "South Dakota",
        "TN": "Tennessee",
        "TX": "Texas",
        "UT": "Utah",
        "VA": "Virginia",
        "VI": "Virgin Islands",
        "VT": "Vermont",
        "WA": "Washington",
        "WI": "Wisconsin",
        "WV": "West Virginia",
        "WY": "Wyoming"
}

cases_csv = pd.read_csv("COVID-19_Cases.csv", dtype=str)
mobil_csv = pd.read_csv("Global_Mobility_Report.csv", dtype=str)
densi_csv = pd.read_csv("StatePopulationDensity.csv", dtype=str)
testi_csv = pd.read_csv("states_daily_4pm_et.csv", dtype=str)
print("## NOTE: Data Has Been Read In...")

cases = cases_csv[cases_csv["Country_Region"] == "US"]
mobil = mobil_csv[mobil_csv["country_region_code"] == "US"]
print("## NOTE: Data Is Now US Only...")

for index, row in mobil.iterrows():
    countyStr = str(row["sub_region_2"])
    countyStr = countyStr.replace(" County", "")
    row["sub_region_2"] = countyStr
print("## NOTE: Data Has Had 'County' Sanitized...")

for index, row in mobil.iterrows():
    countyStr = str(row["sub_region_2"])
    countyStr = countyStr.replace(" Parish", "")
    row["sub_region_2"] = countyStr
print("## NOTE: Data Has Had 'Parish' Sanitized...")

cases = cases.rename(columns={"Province_State": "state", "Admin2": "county", "Date": "date"})
mobil = mobil.rename(columns={"sub_region_1": "state", "sub_region_2": "county"})
densi = densi_csv.rename(columns={"State": "state"})
print("## NOTE: Columns are updated...")

for index, row in cases.iterrows():
    reString = row["date"]
    r1 = re.findall(r"(\d*)/(\d*)/(\d*)", reString)

    month = r1[0][0]
    day = r1[0][1]
    year = r1[0][2]

    if len(month) == 1:
        month = "0" + month

    if len(day) == 1:
        day = "0" + day

    newDate = year + "-" + month + "-" + day

    row["date"] = newDate
print("## NOTE: cases Dates are updated...")

for index, row in testi_csv.iterrows():
    dateString = str(row["date"])

    month = dateString[4:6]
    day = dateString[6:]
    year = dateString[0:4]

    newDate = year + "-" + month + "-" + day

    row["date"] = newDate

print("## NOTE: testi_csv Dates are updated...")

for index, row in mobil.iterrows():
    stateStr = str(row["state"])
    if stateStr == "District of Columbia":
        row["county"] = stateStr

for index, row in cases.iterrows():
    stateStr = str(row["state"])
    if stateStr == "District of Columbia":
        row["county"] = stateStr
print("## NOTE: District of Columbia now has a county (called: District of Columbia)...")

for index, row in testi_csv.iterrows():
    stateStr = str(row["state"])
    row["state"] = states[stateStr]

cases.to_csv("StateCasesData.csv", index=False)
mobil.to_csv("StateMobilityData.csv", index=False)
densi.to_csv("StateDensityData.csv", index=False)
testi_csv.to_csv("StateTestingData.csv", index=False)
print("## NOTE: Files are now written...")