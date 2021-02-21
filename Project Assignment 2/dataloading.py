#!/usr/bin/env python
#
# Copyright 2020 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# =============================================================================
#
# Consume messages from Confluent Cloud
# Using Confluent Python Client for Apache Kafka
#
# =============================================================================

from confluent_kafka import Consumer
import json
import ccloud_lib
import pandas as pd 
import datetime
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError

#Resolves assertions, Cleans data, Transforms data as per DB schema
def clean_df_db_dups(df, tablename, engine, dup_cols=[]):
    args = 'SELECT %s FROM %s' %(', '.join(['"{0}"'.format(col) for col in dup_cols]), tablename)
    df.drop_duplicates(dup_cols, keep='last', inplace=True)
    dbdf = pd.read_sql(args,engine)
    dbdf[dup_cols]=dbdf[dup_cols].apply(pd.to_numeric, errors='coerce')
    df[dup_cols]=df[dup_cols].apply(pd.to_numeric, errors='coerce')
    df = pd.merge(df, dbdf, how='left', on=dup_cols, indicator=True)
    df = df[df['_merge'] == 'left_only']
    df.drop(['_merge'], axis=1, inplace=True)
    return df

def transform_data(raw_data):
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
    raw_data['ACT_TIME'] = raw_data['ACT_TIME'].fillna(0)
    raw_data['ACT_TIME'] = raw_data['ACT_TIME'].astype(int)
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
    try:
        TripDF.to_sql('trip', engine, if_exists='append', index = False)
    except IntegrityError:
        TripDF=clean_df_db_dups(TripDF,"trip",engine,dup_cols=['trip_id'])
        TripDF.to_sql('trip', engine, if_exists='append', index = False,method='multi')

    BreadCrumbDF.to_sql('breadcrumb', engine, if_exists='append', index = False, method='multi')


if __name__ == '__main__':

    # Read arguments and configurations and initialize
    args = ccloud_lib.parse_args()
    config_file = args.config_file
    topic = args.topic
    conf = ccloud_lib.read_ccloud_config(config_file)
    raw_data = pd.DataFrame()
    count = 0

    #create database engine
    engine = create_engine('postgresql://mm:mmuser@localhost:5432/ctran')

    # Create Consumer instance
    # 'auto.offset.reset=earliest' to start reading from the beginning of the
    #   topic if no committed offsets exist
    consumer = Consumer({
        'bootstrap.servers': conf['bootstrap.servers'],
        'sasl.mechanisms': conf['sasl.mechanisms'],
        'security.protocol': conf['security.protocol'],
        'sasl.username': conf['sasl.username'],
        'sasl.password': conf['sasl.password'],
        'group.id': 'python_example_group_1',
        'auto.offset.reset': 'earliest',
    })

    # Subscribe to topic
    consumer.subscribe([topic])

    # Process messages
    total_count = 0
    try:
        while True:
            msg = consumer.poll()
            if msg is None:
                # No message available within timeout.
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("Waiting for message or event/error in poll()")
                continue
            elif msg.error():
                print('error: {}'.format(msg.error()))
            else:
                count = count+1
                # Check for Kafka message
                record_key = msg.key()
                record_value = msg.value()
                data = json.loads(record_value)
                data_df = pd.DataFrame(data, index = [0])
                raw_data = raw_data.append(data_df, ignore_index=True)
                #print(raw_data)
                #count = data['count']
                #total_count += count
                total_count = msg.key()
                #print("Consumed record with key {} and value {}, and updated total count to {}" .format(record_key, record_value, total_count))
                if count == 5000:
                    transform_data(raw_data)
                    raw_data = pd.DataFrame()
                    count = 0

    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()
