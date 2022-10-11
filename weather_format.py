import os
import pandas as pd
from shutil import unpack_archive, rmtree
import csv
from tqdm import tqdm



def format(past=False):
    #-------------------------------------------------------------------------------------------
    #set file paths
    zip_file_path = 'zip_file/'
    data_file_path = 'missing_data/'
    if past:
        zip_file_path = 'past_zip_file/'
        data_file_path = 'data/'
    
    #put the abbriviation and full name of the sites into a dictioanry
    with open('names.csv', 'r') as name_file:
    #-------------------------------------------------------------------------------------------
        names = csv.reader(name_file)
        name_dict = {name[0] : name[1] for name in names}

    if not os.path.isdir(data_file_path):
        os.mkdir(data_file_path)

    for date_zip_file_name in tqdm(os.listdir(zip_file_path)):
        #unzip the file to Weather_sort
        unpack_archive(zip_file_path + date_zip_file_name, data_file_path, 'zip')

        #extrcat the date file name from the zip file (2022-09-22.zip -> 20220922)
        date_file = ''.join(filter(str.isdigit, date_zip_file_name))
        date_file_path = data_file_path + date_file
        
        #iterate through all the files
        for site_file in os.listdir(date_file_path):

            if site_file == 'hka_' + date_file + '.csv':
                new_file_name = 'clk_' + date_file + '.csv'
                os.rename(date_file_path + '/' + site_file, date_file_path + '/' + new_file_name)
                site_file = new_file_name

            if site_file == 'tu1_' + date_file + '.csv':
                new_file_name = 'tun_' + date_file + '.csv'
                os.rename(date_file_path + '/' + site_file, date_file_path + '/' + new_file_name)
                site_file = new_file_name

            site_file_path = date_file_path + '/' + site_file

            if os.path.isdir(site_file_path):
                rmtree(site_file_path)
                continue

            if site_file[:-13] not in name_dict:
                os.remove(site_file_path)
                continue
                
            if os.stat(site_file_path).st_size < 16:
                continue

            df = pd.read_csv(site_file_path, usecols=[0, 1, 2], header=None)
            df.drop([i for i in range(len(df)) if df.iloc[i][0][-1] != '0'], inplace=True)
            df.dropna(inplace=True)
            df[:-1].to_csv(site_file_path, index=None, header=None)



if __name__ == '__main__':
    format()
