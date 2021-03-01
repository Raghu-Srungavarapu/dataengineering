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
    #dropping not necessary rows
    trip_data = raw_data[['trip_No', 'route_number','direction' ]]
    trip_data.columns = ['trip_no', 'route_number','direction']
    
    #Matching data as per db schema
    trip_data = trip_data.replace({'direction':{' 0':'Out',' 1':'Back'}})
    trip_data = trip_data.loc[trip_data['direction'].isin(['Out','Back'])]
    trip_data.drop_duplicates(subset=['trip_no', 'route_number','direction'])
    
    #create temp_table and copy data to temp atbel
    sql3= "drop table temp_table"
    sql2= """create table temp_table (
            trip_no integer,
            route_number integer,
            direction tripdir_type
    );"""
      
    with engine.begin() as conn:     # TRANSACTION
        conn.execute(sql3)
        conn.execute(sql2)    

    trip_data.to_sql('temp_table', engine, index=False, if_exists='append')
    

    with engine.begin() as conn:     # TRANSACTION
        conn.execute(sql)
        
    sql = """
    UPDATE trip AS f
    SET route_id = t.route_number,
    direction = t.direction
    FROM temp_table AS t
    WHERE f.trip_id = t.trip_no
    """

    with engine.begin() as conn:     # TRANSACTION
        conn.execute(sql)



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
                    #raw_data = pd.DataFrame()
                    count = 0

    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()
