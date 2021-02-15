import pandas as pd 
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pylab import rcParams
rcParams['figure.figsize'] = 15, 5

raw_data =pd.read_json(r"C:\Users\jsru2\Desktop\Winter 21\dataset20210120-202701.json")
print(raw_data.head(4))

#Every record should have a Event-No-trip 
num_of_null_rows = sum(raw_data['EVENT_NO_TRIP'].isnull().values.ravel())
if num_of_null_rows==0:
    print("Every record has a Event-No-trip")
else:
    print(f"Assertion  failed!! Every record doesn't have Event-No-trip. Dropping {num_of_null_rows} rows with null values in EVENT_NO_TRIP column", )
    raw_data = raw_data[~raw_data['EVENT_NO_TRIP'].isnull()]
	
#Every record should have an Event-No-stop.
num_of_null_rows = sum(raw_data['EVENT_NO_STOP'].isnull().values.ravel())
if num_of_null_rows==0:
    print("Every record has a Event-No-stop")
else:
    print(f"Assertion  failed!! Every record doesn't have EVENT_NO_STOP. Dropping {num_of_null_rows} rows with null values in EVENT_NO_STOP column", )
    raw_data = raw_data[~raw_data['EVENT_NO_STOP'].isnull()]
	
#The Act_time shouldn’t be empty
num_of_null_rows = sum(raw_data['ACT_TIME'].isnull().values.ravel())
if num_of_null_rows==0:
    print("Every record has a ACT_TIME")
else:
    print(f"Assertion  failed!! Every record doesn't have ACT_TIME. Dropping {num_of_null_rows} rows with null values in ACT_TIME column", )
    raw_data = raw_data[~raw_data['ACT_TIME'].isnull()]


#Every record should have a latitude value other than 0
num_of_null_rows = sum(raw_data['GPS_LATITUDE'].isnull().values.ravel())
if num_of_null_rows==0:
    print("Every record has a GPS_LATITUDE")
else:
    print(f"Assertion  failed!! Every record doesn't have GPS_LATITUDE. Dropping {num_of_null_rows} rows with null values in GPS_LATITUDE column", )
    raw_data = raw_data[~raw_data['GPS_LATITUDE'].isnull()]

num_zero_value_rows = (raw_data['GPS_LATITUDE']=='0').values.ravel().sum()
if num_zero_value_rows==0:
    print("All records have GPS_LATITUDE value other than 0")
else:
    print(f"Assertion  failed!! Few records don't have non-zero GPS_LATITUDE. Dropping {num_zero_value_rows} rows with GPS_LATITUDE value as 0", )
    raw_data = raw_data[raw_data.GPS_LATITUDE!='0']

#Every record should have a longitude value other than 0
num_of_null_rows = sum(raw_data['GPS_LONGITUDE'].isnull().values.ravel())
if num_of_null_rows==0:
    print("Every record has a GPS_LONGITUDE")
else:
    print(f"Assertion  failed!! Every record doesn't have GPS_LONGITUDE. Dropping {num_of_null_rows} rows with null values in GPS_LONGITUDE column", )
    raw_data = raw_data[~raw_data['GPS_LONGITUDE'].isnull()]

num_zero_value_rows = (raw_data['GPS_LONGITUDE']=='0').values.ravel().sum()
if num_zero_value_rows==0:
    print("All records have GPS_LONGITUDE value other than 0")
else:
    print(f"Assertion  failed!! Few records don't have non-zero GPS_LONGITUDE. Dropping {num_zero_value_rows} rows with GPS_LONGITUDE value as 0", )
    raw_data = raw_data[raw_data.GPS_LONGITUDE!='0']


#create a time stamp column
raw_data['date']=pd.to_datetime(raw_data['OPD_DATE'], format='%d-%b-%y')
raw_data['time']=pd.to_timedelta(raw_data['ACT_TIME'], unit='s')
# print(raw_data.head(4))
raw_data['tstamp'] = raw_data['date'] + raw_data['time']

#create speed column. Convert meter/sec to Miles/Hour multiply by 2.237
raw_data['speed'] = raw_data['VELOCITY']
raw_data['speed'] = pd.to_numeric(raw_data['speed'], errors='coerce')
raw_data['speed'].fillna(0, inplace = True)
raw_data['speed']= raw_data['speed']*2.237

#Create dataframe for BreadCrumb table
BreadCrumbDF= raw_data[['tstamp','GPS_LATITUDE','GPS_LONGITUDE','DIRECTION','speed','EVENT_NO_TRIP']]
BreadCrumbDF.columns = ['tstamp','latitude','longitude', 'direction', 'speed','trip_id'  ]
print(BreadCrumbDF.head(4))

