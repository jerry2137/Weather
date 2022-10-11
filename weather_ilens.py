import requests
import os
from datetime import datetime
from tqdm import tqdm


#download the zipped weather data of the required dates from ilens
def ilens(dates, past=False):

    for date in tqdm(dates):
        url = r'https://i-lens.hk/hkweather/file_download.php?file_type=history_minute_record&file={date}'.format(date = date.strftime('%Y-%m-%d'))
        zip_file = requests.get(url)
        #-------------------------------------------------------------------------------------------
        save_path = 'zip_file/'
        if past:
            save_path = 'past_zip_file/'
        #-------------------------------------------------------------------------------------------
        #create a directory when date changes
        if not os.path.isdir(file_path + today_str):
            os.mkdir(save_path)
            
        file_name = url.split("=")[2] + '.zip'
        file_save_path = save_path + file_name
        with open(file_save_path, 'wb') as f:
            f.write(zip_file.content)

    return 0


if __name__ == '__main__':
    ilens([datetime.strptime('2022-09-26', '%Y-%m-%d')])
