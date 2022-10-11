from cmath import nan
import requests
import pandas as pd
import io
import time
import csv
from datetime import date, timedelta, datetime
import os

#-----------------------------------------------------------------------------------------------
import weather_missing
#-----------------------------------------------------------------------------------------------


def new_day(file_path):
  today = date.today()
  today_str = today.strftime('%Y%m%d')
      
  #create a directory when date changes
  if not os.path.isdir(file_path + today_str):
    os.mkdir(file_path + today_str)

  #fill the missing data of the day before yesterday(the data of yesterday might not be ready yet)
  last_last_day = today - timedelta(days = 2)
  weather_missing.fill(last_last_day, last_last_day)



def update():

  #-------------------------------------------------------------------------------------------
  file_path = 'data/'

  #put the abbreviation and full name of the sites into a dictioanry
  #with open('names.csv', 'r') as name_file:
  with open('names.csv', 'r') as name_file:
  #-------------------------------------------------------------------------------------------
    names = csv.reader(name_file)
    name_dict = {name[0] : name[1] for name in names}
  
  #two dictionaries of flags indicating whether the data is changed or not
  current_temperature_timestamp = dict.fromkeys(name_dict.keys(), 0)
  current_humidity_timestamp = dict.fromkeys(name_dict.keys(), 0)

  #initialize today to be yesterday
  today_str = (date.today() - timedelta(days = 1)).strftime('%Y%m%d')


  #keep downloading data from HKO for every 10 mins
  while True:

    #download data
    temperature_data = requests.get(r'https://data.weather.gov.hk/weatherAPI/hko_data/regional-weather/latest_1min_temperature.csv').content
    temperature_df = pd.read_csv(io.StringIO(temperature_data.decode('utf-8')))
    humidity_data = requests.get(r'https://data.weather.gov.hk/weatherAPI/hko_data/regional-weather/latest_1min_humidity.csv').content
    humidity_df = pd.read_csv(io.StringIO(humidity_data.decode('utf-8')))
    
    data_date = str(temperature_df.iloc[1, 0])[:8]
    if data_date != today_str:
      new_day(file_path)
      today_str = data_date
      
    
    #update data
    for n in name_dict:
      row_temperature = temperature_df.loc[temperature_df['Automatic Weather Station'] == name_dict[n]]
      row_humidity = humidity_df.loc[humidity_df['Automatic Weather Station'] == name_dict[n]]

      new_temperature = nan
      new_humidity = nan

      if (temperature_df.iloc[1, 0] != current_temperature_timestamp[n]) & (not row_temperature.empty):
        new_temperature = row_temperature.iloc[0, 2]
        current_temperature_timestamp[n] = temperature_df.iloc[1, 0]
        
      if (humidity_df.iloc[1, 0] != current_humidity_timestamp[n]) & (not row_humidity.empty):
        new_humidity = row_humidity.iloc[0, 2]
        current_humidity_timestamp[n] = humidity_df.iloc[1, 0]
      
      time_object = datetime.strptime(str(current_temperature_timestamp[n]), '%Y%m%d%H%M')
      #-----------------------------------------------------------------------------------------
      #might be %-H in some other version
      date_time = time_object.strftime('%Y/%m/%d %#H:%M')
      #-----------------------------------------------------------------------------------------
      new_row_df = pd.DataFrame([date_time, new_temperature, new_humidity])
      new_row_df.T.to_csv(file_path + today_str + '/' + n + '_' + today_str + '.csv', mode='a', header=False, index=False)
      
    print('downloaded')
    #wait for 10 minutes
    time.sleep(600)


if __name__ == '__main__':
  update()
