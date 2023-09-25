
# Compare results between the table outputs and data filtered from original table loaded from Postgresql database
import numpy as np
datdf = dat.replace(-9999, np.NaN)
weather_stations.loc[(weather_stations['year'] == 2014) & (weather_stations['station_id'] == 'USC00339312'), 'precipitation'].sum()


mean_tempearture.loc[(weather_stations['year'] == 2014) & (weather_stations['station_id'] == 'USC00339312'), 'max_temperature'].mean()