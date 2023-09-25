# Import libraries
import numpy as np
import pandas as pd
from sqlalchemy import create_engine


# Read columns except datelocation_id to a dataframe (replace your username and password)
engine = create_engine('postgresql+psycopg2://tharinduabeysinghe:password@localhost/weather')
query = '''SELECT date, max_temperature, min_temperature, precipitation, station_id
           FROM weather_data
           '''
data = pd.read_sql_query(query, con=engine)
weather_stations = pd.DataFrame(data)

# Add a year column
weather_stations['year'] = pd.DatetimeIndex(weather_stations['date']).year
weather_stations

# Filter data to a new datafram and replace -9999 with NaN
weather_subset = weather_stations[['max_temperature', 'min_temperature', 'year', 'station_id']]
weather_subset = weather_subset.replace(-9999, np.NaN)

# Group weather_subset by year and station, calculate mean tempreatures
grouped = weather_subset.groupby(by=['year','station_id'])
mean_tempearture = grouped.mean().reset_index()
mean_tempearture = mean_tempearture.rename(columns={
          'max_temperature': 'avg_max_temperature', 
          'min_temperature': 'avg_min_temperature'
                                  }, errors='raise')

# Calculate total accumulated precipitation by year, by station
precipitation_daily = weather_stations[['precipitation', 'year', 'station_id']]
precipitation_daily = precipitation_daily.replace(-9999, np.NaN)
grouped_precipitation_daily = precipitation_daily.groupby(by=['year','station_id'])
precipitation_accumulation = grouped_precipitation_daily.sum().reset_index()
precipitation_accumulation = precipitation_accumulation.rename(columns={
                            'precipitation': 'accumulate_precipitation'}, errors='raise')
