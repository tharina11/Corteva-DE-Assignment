# Import libraries
import os
import numpy as np
import pandas as pd
from datetime import datetime
import logging
import psycopg2
psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)

# Define logging level
logging.basicConfig(filename = 'weather_data_ingest.log', level=logging.DEBUG)

# Get file names from directory
file_list=os.listdir(r'code-challenge-template-main/wx_data')

# Read all text files and create a dataframe, create a new unique identifier column
dataframes = []
for file in file_list:
    filename = 'code-challenge-template-main/wx_data/{}'.format(file)
    file_data = pd.read_csv(filename, sep='\t', header=None, on_bad_lines='skip')
    file_data.columns = ['date', 'max_temperature', 'min_temperature', 'precipitation']
    file_data['station_id'] = file.split('.')[0]
    file_data['datelocation_id'] = file_data['date'].astype(str) + file_data['station_id']
    dataframes.append(file_data)
weather_stations = pd.concat(dataframes, axis=0)

# Update date format, set date column as index
weather_stations['date']= pd.to_datetime(weather_stations['date'],format='%Y%m%d')
weather_stations.set_index('date', inplace=True)
weather_stations.index = np.datetime_as_string(weather_stations.index, unit='D')

#Convert dataframe to a list of tuples
records = weather_stations.to_records(index=True)

# Define username and password of the Posgresql database (use your password)
username = 'tharinduabeysinghe'
password = '12345'

#Create weather database
conn = psycopg2.connect(
   database='postgres', user= username, password= password, host='127.0.0.1', port= '5432'
)
conn.autocommit = True
cursor = conn.cursor()
create_query = '''SELECT 'CREATE DATABASE weather'
                  WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'weather');''';
cursor.execute(create_query)
conn.close()

# Create weather_data table in the weather database
conn = psycopg2.connect(
    database="weather", user= username, password= password, host='127.0.0.1', port= '5432'
)
conn.autocommit = True
cur = conn.cursor()
cur.execute('''CREATE TABLE IF NOT EXISTS weather_data
               (
                date DATE,
                max_temperature INT,
                min_temperature INT,
                precipitation INT,
                station_id VARCHAR(50),
                datelocation_id VARCHAR(50),
                UNIQUE (datelocation_id)
                );''')             
conn.close()

# Ingest data of the dataframe into the weather_data table and write data to the log file
conn = psycopg2.connect(
    database='weather', user= username, password= password, host='127.0.0.1', port= '5432'
)
conn.autocommit = True
cur = conn.cursor()
logging.debug('Start Time: {}'.format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
insert_query = '''
INSERT INTO weather_data (date, max_temperature, min_temperature, precipitation, station_id, datelocation_id)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING;'''
cur = conn.cursor()
cur.executemany(insert_query, records)
logging.debug('End Time: {}'.format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
row_count = cur.rowcount
logging.debug('number of records ingested: {}'.format(row_count))
conn.close()