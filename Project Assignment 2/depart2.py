import pandas as pd 
import datetime
import numpy as np
import psycopg2
from sqlalchemy import create_engine

#create database engine
engine = create_engine('postgresql://mm:mmuser@localhost:5432/breadcrumb')

#TODO change the way you read data
raw_data =pd.read_json(r"C:\Users\jsru2\Desktop\Winter 21\dataset20210120-202701.json")

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
#print(raw_data.head(4))

#Create service_key
raw_data['service_key'] = raw_data['date'].dt.dayofweek
raw_data.loc[raw_data['service_key'].isin(range(0,5)),'service_key']='Weekday'
raw_data.loc[raw_data['service_key']==5,'service_key']='Saturday'
raw_data.loc[raw_data['service_key']==6,'service_key']='Sunday'


#Create dataframe for BreadCrumb table
BreadCrumbDF= raw_data[['tstamp','GPS_LATITUDE','GPS_LONGITUDE','DIRECTION','speed','EVENT_NO_TRIP']]
BreadCrumbDF.columns = ['tstamp','latitude','longitude', 'direction', 'speed','trip_id'  ]
#print(BreadCrumbDF.head(4))

#Convert column datatypes to match DB schema
cols = BreadCrumbDF.columns.drop(['tstamp','speed','trip_id'])
BreadCrumbDF[cols] = BreadCrumbDF[cols].apply(pd.to_numeric, errors='coerce')

#Create dataframe for Trip table
TripDF = raw_data[['EVENT_NO_TRIP', 'VEHICLE_ID','service_key']]
TripDF.columns= ['trip_id','vehicle_id','service_key']
print(TripDF.head(4))

#Create dataframe for Trip table
TripDF = raw_data[['EVENT_NO_TRIP', 'VEHICLE_ID','service_key']]
TripDF.columns= ['trip_id','vehicle_id','service_key']
#Drop duplicate entries of Trips and keep the fisrt entry
TripDF=TripDF.drop_duplicates()

#Write data to the tables
TripDF.to_sql('trip', engine, if_exists='append', index = False)
BreadCrumbDF.to_sql('breadcrumb', engine, if_exists='append', index = False)