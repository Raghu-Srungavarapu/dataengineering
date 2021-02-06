import pandas as pd 
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pylab import rcParams
rcParams['figure.figsize'] = 15, 5

#read data from raw file
raw_data = pd.read_excel(r"C:\Users\jsru2\Desktop\Winter 21\Oregon Hwy 26 Crash Data for 2019.xlsx")

#Devide the data as per their record types
CrashesDF = raw_data[raw_data['Record Type'] == 1]
VehiclesDF = raw_data[raw_data['Record Type'] == 2]
ParticipantsDF = raw_data[raw_data['Record Type'] == 3]

CrashesDF = CrashesDF.dropna(axis=1,how='all')
VehiclesDF = VehiclesDF.dropna(axis=1,how='all')
ParticipantsDF = ParticipantsDF.dropna(axis=1,how='all')

#The following sections test the assumptions and corrects the data if the assumptions fail.

#Every Crash record has a Crash ID.
if CrashesDF['Crash ID'].isnull().values.any() == False:
    print("Every Crash record has a Crash ID")
else:
    print("Assertion  failed!! Every Crash record doesn't have a Crash ID. Dropping columns with null values in Crash ID columns")
    CrashesDF = CrashesDF[~CrashesDF['Crash ID'].isnull()] 
	
#Every participant record has Vehicle ID
if ParticipantsDF['Participant ID'].isnull().values.any() == False:
    print("Every participant record has Vehicle ID")
else:
    print("Assertion failed!! Every participant record doesn't have a Vehicle ID. Dropping columns with null values in Vehicle ID columns")
    ParticipantsDF = ParticipantsDF[~ParticipantsDF['Participant ID'].isnull()]
	
#Crash Month  is between 01 and 12
month_range = range(1,13)
rslt_df = CrashesDF.loc[~CrashesDF['Crash Month'].isin(month_range)]
if len(rslt_df)==0:
    print("All Crash Months are between 01 and 12")
else:
    print("Assertion failed!! All Crash Months are not between 01 and 12. Dropping rows that failed condition")
    CrashesDF = CrashesDF.loc[CrashesDF['Crash Month'].isin(month_range)]
	
#Crash Day is between 01-31
date_range = range(1,32)
rslt_df = CrashesDF.loc[~CrashesDF['Crash Day'].isin(date_range)]
if len(rslt_df)==0:
    print("All Crash Days are between 01 and 12")
else:
    print("Assertion failed!! All Crash Days are not between 01 and 31. Dropping rows that failed condition")
    CrashesDF = CrashesDF.loc[CrashesDF['Crash Day'].isin(month_range)]
    print(CrashesDF)
	
#Highway Component must be null when the Highway Number is null
rslt_df = CrashesDF[CrashesDF['Highway Number'].isnull() & ~CrashesDF['Highway Component'].isnull()]
if len(rslt_df)==0:
    print("Highway Component is null when the Highway Number is null")
else:
    print("Assertion failed!! Highway Component is not null when the Highway Number is null. Dropping rows that failed condition")
    CrashesDF.drop(CrashesDF[CrashesDF['Highway Number'].isnull() & ~CrashesDF['Highway Component'].isnull()].index, inplace = True)

#Roadway Number must be null when the Highway Number is null
rslt_df = CrashesDF[CrashesDF['Highway Number'].isnull() & ~CrashesDF['Roadway Number'].isnull()]
if len(rslt_df)==0:
    print("Roadway Number is null when the Highway Number is null")
else:
    print("Assertion failed!! Roadway Number is not null when the Highway Number is null. Dropping rows that failed condition")
    CrashesDF.drop(CrashesDF[CrashesDF['Highway Number'].isnull() & ~CrashesDF['Roadway Number'].isnull()].index, inplace = True)


#For any given crash the number of Unique Vehicle IDs (Number of participating vehicles) 
#should be equal to the Maximum value seen in the Vehicle Coded Seq# column.
assertion_check = True
for id in VehiclesDF['Crash ID'].unique():
    rslt_df = VehiclesDF[VehiclesDF['Crash ID']==id]
    if len(rslt_df)!=rslt_df['Vehicle Coded Seq#'].max():
        print("Assertion Failed!! Cleaning Data")
        VehiclesDF.drop(VehiclesDF[VehiclesDF['Crash ID']==id].index(), inplace = True)
        assertion_check = False

if assertion_check == True:
    print("Assertion True. All the vegicles involved in the crash are listed")

#Every vehicle has a unique Vehicle ID
if len(VehiclesDF.index)==len(VehiclesDF['Vehicle ID'].unique()):
    print("Every vehicle has a unique Vehicle ID")
else:
    print("Assertion failed. Every vehicle doesn't have a unique Vehicle ID. Removing both duplicate entries")
    VehiclesDF.drop_duplicates(subset=['Vehicle ID'], keep= False)

#Every Participant had a unique Participant ID.
if len(ParticipantsDF.index)==len(ParticipantsDF['Participant ID'].unique()):
    print("Every Participant has a unique Participant ID")
else:
    print("Assertion failed. Every Participant doesn't have a unique Vehicle ID. Removing both duplicate entries")
    ParticipantsDF.drop_duplicates(subset=['Participant ID'], keep= False)

#Every Vehicle involved in a crash has a known Crash ID
known_crash_id = CrashesDF['Crash ID'].unique()
rslt_df = VehiclesDF.loc[~VehiclesDF['Crash ID'].isin(known_crash_id)]
if len(rslt_df)==0:
    print("Every Vehicle involved in a crash has a known Crash ID")
else:
    print("Assertion Failed!! Every Vehicle involved in a crash doesn't have a known Crash ID. Dropping such Vehicles")
    VehiclesDF=VehiclesDF.loc[VehiclesDF['Crash ID'].isin(known_crash_id)]

#Every crash participant has a Vehicle ID of a known Vehicle.
known_vehicle_id = VehiclesDF['Vehicle ID'].unique()
rslt_df = ParticipantsDF.loc[~ParticipantsDF['Vehicle ID'].isin(known_vehicle_id)]
if len(rslt_df)==0:
    print("Every Vehicle involved in a crash has a known Crash ID")
else:
    print("Assertion Failed!! Every Vehicle involved in a crash doesn't have a known Crash ID. Dropping such Vehicles")
    ParticipantsDF=ParticipantsDF.loc[ParticipantsDF['Vehicle ID'].isin(known_vehicle_id)]

#Crashes are High during weekends
CrashesDF.describe(include=[np.number])
CrashesDF.boxplot(column='Week Day Code')
plt.grid(True, axis='y')
plt.ylabel('Week day Code')
plt.xticks([1], ['Crash ID'])

#Crashes are high during morning (8 am to 11 am)and evening hours (4pm - 7pm
CrashesDF = CrashesDF.loc[CrashesDF['Crash Hour'].isin(range(1,25))]
x = CrashesDF['Crash Hour']
ax = ax = sns.distplot(x, hist=True, kde=True, rug=False, color='m', bins=25, hist_kws={'edgecolor':'black'})
plt.show()


