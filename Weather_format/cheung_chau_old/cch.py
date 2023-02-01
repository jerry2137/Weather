from pathlib import Path
from datetime import datetime
import os
import pandas as pd
import shutil
from tqdm import tqdm


dates = pd.date_range(start = '2017-08-07', end = '2022-01-16', freq = '1D')

dates = [datetime.strftime(x, '%F') for x in dates]

date_str = []
for date in dates:
    date = date.replace("-", "")
    date_str.append(date)

file_paths = []
for date in date_str:
    file_name = 'hko_' + date + '.csv'
    file_path = r'D:\test_development\Weather\Weather_sort\hko\hko_data'
    file_path = os.path.join(file_path, file_name)
    file_paths.append(file_path)

weathers_main_df = pd.read_csv(r'D:\test_development\Weather\Weather_sort\hko\hko_data\hko_20170806.csv', usecols = ([0, 1, 2]))

for i in range(1441): # 1441 is the total number of time interval. 
    if(int(weathers_main_df['Date'][i].split(":")[1])/10).is_integer() == False:
        weathers_main_df.drop(i, inplace = True)

weathers_main_df.reset_index(drop = True, inplace = True)

error_message = []
for file in tqdm(file_paths):
    try:
        weathers_temp_df = pd.read_csv(file, usecols = ([0, 1, 2]))
        for i in range(len(weathers_temp_df.index)):
            if(int(weathers_temp_df['Date'][i].split(":")[1])/10).is_integer() == False:
                weathers_temp_df.drop(i, inplace = True)
        weathers_main_df = weathers_main_df.append(weathers_temp_df)
    except:
        error_message.append(file)

weathers_main_df.drop_duplicates(inplace=True)
weathers_main_df.reset_index(drop=True, inplace = True)
weathers_main_df.to_csv('weather_hko.csv', index = False)

print(error_message)
