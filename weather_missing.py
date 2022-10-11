from datetime import datetime, timedelta
import heapq
import os
import pandas as pd
from pandas import date_range
import csv
from collections import OrderedDict
import shutil

#-------------------------------------------------------------------------------------------
#import weather_ilens
#import weather_format
#call other script
import importlib.util
import sys

spec = importlib.util.spec_from_file_location("format", "D:/test_development/Weather/Weather_format/weather_format.py")
weather_format = importlib.util.module_from_spec(spec)
sys.modules["format"] = weather_format
spec.loader.exec_module(weather_format)

spec = importlib.util.spec_from_file_location("ilens", "D:/test_development/Weather/Weather_down/weather_ilens.py")
weather_ilens = importlib.util.module_from_spec(spec)
sys.modules["ilens"] = weather_ilens
spec.loader.exec_module(weather_ilens)
#-------------------------------------------------------------------------------------------


#get all missing time(not necessary for now)
def all_missing(dates):
    #-------------------------------------------------------------------------------------------
    data_file_path = "D:/test_development/Weather/Weather_update/data/"
    #-------------------------------------------------------------------------------------------

    all_missing ={}

    for date in dates:
        date_str = date.strftime('%Y%m%d')

        if not os.path.isdir(data_file_path + date_str):
            os.mkdir(data_file_path + date_str)

        time_period = date_range(start = date, end = date + timedelta(days=1), freq = '10t')

        file_path = data_file_path + date_str

        missing = {}

        for file_name in os.listdir(file_path):
            #create an ordered dictionary to store the missing time
            missing_ordereddict = OrderedDict()

            #put the current data in to data_list
            with open(file_path + file_name, 'r') as date_file:
                date_file = csv.reader(date_file)

                data_list = [row for row in date_file]


            data_limit = len(data_list)
            data_count = 0

            time_limit = len(time_period)
            time_count = 0

            #compare the donwloaded data and the required time interval
            while True:

                #end of the data
                if data_count == data_limit:
                    for i in range(time_count, time_limit):
                        missing_ordereddict[time_period[i]] = True
                    break

                #end of the time interval
                if time_count == time_limit:
                    break

                if len(data_list[data_count]) == 0:
                    continue
                
                #load data
                data = datetime.strptime(data_list[data_count][0], '%Y/%m/%d %H:%M')
                time = time_period[time_count]

                time_count += 1
                
                #check if the data and the required time is closer than 10 mins
                if data - time < timedelta(minutes=10):
                    if (data_list[data_count][1] != '') & (data_list[data_count][2] != ''):
                        missing_ordereddict[time] = False
                    else:
                        missing_ordereddict[time] = True
                    data_count += 1
                    
                else:
                    missing_ordereddict[time] = True

            #put all the missing time into the missing dictionary as a list with key of the site
            missing[file_name] = missing_ordereddict

        all_missing[date] = missing

    return all_missing


#print the missing interval and check manually(not necessary for now)
def check_boundaries(missing):
    
    for site, data in missing.items():
        print(site)

        boundaries = []
        begin_missing = False

        for time, flag in data.items():
            if flag != begin_missing:
                boundaries.append(time)
                begin_missing = flag

        last_data = data.popitem(last=True)
        if last_data[0] != boundaries[-1]:
            boundaries.append(last_data[0])

        print(boundaries)


#check if there's any data lackage
def check(dates):
    #-------------------------------------------------------------------------------------------
    #data_file_path = "data/"
    data_file_path = "D:/test_development/Weather/Weather_update/data/"
    #-------------------------------------------------------------------------------------------

    missing_date = []

    for date in dates:
        date_str = date.strftime('%Y%m%d')

        file_path = data_file_path + date_str

        #if the file doesn't exist, return this date
        if not os.path.isdir(file_path):
            missing_date.append(date)
            continue

        #if the file is empty, return this date
        if not os.listdir(file_path):
            missing_date.append(date)
            continue
            
        #----------------------------------------------------------------------------------------------
        #use inclusive insteade of closed in the lastest version
        time_period = date_range(start=date, end=date + timedelta(days=1), freq='10t', closed='left')
        #----------------------------------------------------------------------------------------------

        for file_name in os.listdir(file_path):

            #put the current data in to data_list of this date
            with open(file_path + '/' + file_name, 'r') as date_file:
                date_file = csv.reader(date_file)
                data_list = [row for row in date_file]

            file_missing = False

            #compare the donwloaded data and the required time interval
            for count in range(len(data_list)):
                #load data
                data = datetime.strptime(data_list[count][0], '%Y/%m/%d %H:%M')

                #check if the data and the required time is closer than 10 mins
                if data - time_period[count] >= timedelta(minutes=10):
                    file_missing = True
                    break

                #check if the file is complete
                if (data_list[count][1] == '') | (data_list[count][2] == ''):
                    file_missing = True
                    break

            if file_missing:
                missing_date.append(date)
                break

    return missing_date


#combine the old data with the newly downloaded data
def combine():

    #-------------------------------------------------------------------------------------------
    #data_file_path = 'data/'
    #missing_file_path = 'missing_data/'
    data_file_path = "D:/test_development/Weather/Weather_update/data/"
    missing_file_path = "D:/test_development/Weather/Weather_update/missing_data/"
    #-------------------------------------------------------------------------------------------

    #iterate through all the dates
    for date in os.listdir(missing_file_path):
        missing_path = missing_file_path + date + '/'
        date_path = data_file_path + date + '/'

        #if the file does not exist, move the data directly
        if not os.path.isdir(date_path):
            os.rename(missing_path, date_path)
            continue

        #if the file is empty, remove the empty file and move the data directly
        if not os.listdir(date_path):
            shutil.rmtree(date_path)
            os.rename(missing_path, date_path)
            continue

        #iterate through all the sites on that day
        for site_name in os.listdir(missing_path):

            missing_site_path = missing_path + site_name
            date_site_path = date_path + site_name

            #load both the old data and the new data
            df1 = pd.read_csv(missing_site_path, header=None)
            df2 = pd.read_csv(date_site_path, header=None)

            #use the last step of merge sort(O(n)) to combine two data
            #the data will generate index:'_0', '_1', '_2' after coming out from the generator heapq.merge()
            df = pd.DataFrame(heapq.merge(df1.itertuples(index=False), df2.itertuples(index=False)))

            #drop duplcates time
            df.drop_duplicates('_0', inplace=True)

            #store the data
            df.to_csv(date_site_path, index=None, header=None)

    #delete all the temporary files in missing_data
    for missing_file_name in os.listdir(missing_file_path):
        shutil.rmtree(missing_file_path + missing_file_name)



def fill(start_date, end_date):
    dates = date_range(start = start_date, end = end_date, freq = '1D')

    #get the missing dates
    missing = check(dates)

    #call weather_ilens to download the missing data from ilens
    weather_ilens.ilens(missing)

    #format the file to suit the other files from HKO
    weather_format.format()

    #combine the old data with the newly downloaded data
    combine()

    #-------------------------------------------------------------------------------------------
    #delete the zip files
    #zip_file_path = 'zip_file/'
    zip_file_path = 'D:/test_development/Weather/Weather_down/zip_file/'
    for date_zip_file_name in os.listdir(zip_file_path):
        os.remove(zip_file_path + date_zip_file_name)
    #-------------------------------------------------------------------------------------------



        
if __name__ == '__main__':
    combine()

    #check_boundaries(check())