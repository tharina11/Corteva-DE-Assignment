# Import libraries
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
import psycopg2
psycopg2.extensions.register_adapter(np.int32, psycopg2._psycopg.AsIs)

# Read columns except datelocation_id to a dataframe
engine = create_engine('postgresql+psycopg2://tharinduabeysinghe:#####@localhost/weather')
query = '''SELECT date, max_temperature, min_temperature, precipitation, station_id
           FROM weather_data
           '''
data = pd.read_sql_query(query, con=engine)
weather_stations = pd.DataFrame(data)

# Add a year column
weather_stations['year'] = pd.DatetimeIndex(weather_stations['date']).year
weather_stations

# Calculate mean tempreatures by year, by station
weather_subset = weather_stations[['max_temperature', 'min_temperature', 'year', 'station_id']]
weather_subset = weather_subset.replace(-9999, np.NaN)
grouped = weather_subset.groupby(by=['year','station_id'])
mean_tempearture = grouped.mean().reset_index()
mean_tempearture = mean_tempearture.rename(columns={'max_temperature': 'avg_max_temperature', 
                                                    'min_temperature': 'avg_min_temperature'
                                                    })
mean_tempearture['yearstation_id'] = mean_tempearture['year'].astype(str) + mean_tempearture['station_id']
mean_tempearture.set_index('yearstation_id', inplace=True)

# Calculate total accumulated precipitation by year, by station
precipitation_daily = weather_stations[['precipitation', 'year', 'station_id']]
precipitation_daily = precipitation_daily.replace(-9999, np.NaN)
grouped_precipitation_daily = precipitation_daily.groupby(by=['year','station_id'])
precipitation_accumulation = grouped_precipitation_daily.sum().reset_index()
precipitation_accumulation = precipitation_accumulation.rename(columns={'precipitation': 'accumulate_precipitation'})
precipitation_accumulation['yearstation_id'] = precipitation_accumulation['year'].astype(str) + precipitation_accumulation['station_id']
precipitation_accumulation.set_index('yearstation_id', inplace=True)

# Convert dataframes to tuples
temperature_records = mean_tempearture.to_records(index=True)
precipitation_records = precipitation_accumulation.to_records(index=True)

# Define username and password of the Posgresql database (use your password)
username = 'tharinduabeysinghe'
password = '#####'

# Create average_temperatures table in the weather database
conn = psycopg2.connect(
    database="weather", user= username, password= password, host='127.0.0.1', port= '5432'
)
conn.autocommit = True
cur = conn.cursor()
cur.execute('''CREATE TABLE IF NOT EXISTS average_temperatures
               (
                yearstation_id VARCHAR(50),
                year INT,
                station_id VARCHAR(50),
                avg_max_temperature FLOAT,
                avg_min_temperature FLOAT,
                UNIQUE (yearstation_id)
                );''')             
conn.close()

# Ingest data of the dataframe into the average_temperatures table
conn = psycopg2.connect(
    database="weather", user= username, password= password, host='127.0.0.1', port= '5432'
)
conn.autocommit = True
cur = conn.cursor()
insert_query = '''
INSERT INTO average_temperatures (yearstation_id, year, station_id, avg_max_temperature, avg_min_temperature)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING;'''
cur = conn.cursor()
cur.executemany(insert_query, temperature_records)
conn.close()

# Create precipitation_accumulation table in the weather database
conn = psycopg2.connect(
    database="weather", user= username, password= password, host='127.0.0.1', port= '5432'
)
conn.autocommit = True
cur = conn.cursor()
cur.execute('''CREATE TABLE IF NOT EXISTS precipitation_accumulation
               (
                yearstation_id VARCHAR(50),
                year INT,
                station_id VARCHAR(50),
                accumulate_precipitation FLOAT,
                UNIQUE (yearstation_id)
                );''')             
conn.close()

# Ingest data of the dataframe into the precipitation_accumulation table
conn = psycopg2.connect(
    database="weather", user= username, password= password, host='127.0.0.1', port= '5432'
)
conn.autocommit = True
cur = conn.cursor()
insert_query = '''
INSERT INTO precipitation_accumulation (yearstation_id, year, station_id, accumulate_precipitation)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT DO NOTHING;'''
cur = conn.cursor()
cur.executemany(insert_query, precipitation_records)
conn.close()