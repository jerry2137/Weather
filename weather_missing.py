from datetime import datetime, timedelta
import heapq
import os
import pandas as pd
from pandas import date_range
import csv
from collections import OrderedDict
import shutil

#-------------------------------------------------------------------------------------------
import weather_ilens
import weather_format
#-------------------------------------------------------------------------------------------


#check if there's any data lackage
def check(dates):
    #-------------------------------------------------------------------------------------------
    data_file_path = "data/"
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
    data_file_path = 'data/'
    missing_file_path = 'missing_data/'
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
    zip_file_path = 'zip_file/'
    for date_zip_file_name in os.listdir(zip_file_path):
        os.remove(zip_file_path + date_zip_file_name)
    #-------------------------------------------------------------------------------------------



        
if __name__ == '__main__':
    combine()

    #check_boundaries(check())
