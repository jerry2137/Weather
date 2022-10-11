import requests
import os
from datetime import datetime


#download the zipped weather data of the required dates from ilens
def ilens(dates, past=False):

    for date in dates:
        url = r'https://i-lens.hk/hkweather/file_download.php?file_type=history_minute_record&file={date}'.format(date = date.strftime('%Y-%m-%d'))
        zip_file = requests.get(url)
        #-------------------------------------------------------------------------------------------
        #save_path = 'zip_file/'
        save_path = r'D:\test_development\Weather\Weather_down\zip_file/'
        if past:
            #save_path = 'past_zip_file/'
            save_path = r'D:\test_development\Weather\Weather_down\past_zip_file/'
        #-------------------------------------------------------------------------------------------
        file_name = url.split("=")[2] + '.zip'
        file_save_path = os.path.join(save_path, file_name)
        with open(file_save_path, 'wb') as f:
            f.write(zip_file.content)

    return 0


if __name__ == '__main__':
    ilens([datetime.strptime('2022-09-26', '%Y-%m-%d')])