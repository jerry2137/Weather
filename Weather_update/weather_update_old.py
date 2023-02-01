from datetime import date
from pandas.io.sql import DatabaseError
import requests
import pandas as pd
import io
import time

current_hko_temp_timestamp = 0
current_hko_humidity_timestamp = 0

current_wlp_temp_timestamp = 0
current_wlp_humidity_timestamp = 0

current_hks_temp_timestamp = 0
current_hks_humidity_timestamp = 0

current_kp_temp_timestamp = 0
current_kp_humidity_timestamp = 0

current_sha_temp_timestamp = 0
current_sha_humidity_timestamp = 0

current_skg_temp_timestamp = 0
current_skg_humidity_timestamp = 0

current_wgl_temp_timestamp = 0
current_wgl_humidity_timestamp = 0

current_tun_temp_timestamp = 0
current_tun_humidity_timestamp = 0

current_tw_temp_timestamp = 0
current_tw_humidity_timestamp = 0

current_twn_temp_timestamp = 0
current_twn_humidity_timestamp = 0

current_ty1_temp_timestamp = 0
current_ty1_humidity_timestamp = 0

current_jkb_temp_timestamp = 0
current_jkb_humidity_timestamp = 0

current_tpo_temp_timestamp = 0
current_tpo_humidity_timestamp = 0

current_tkl_temp_timestamp = 0
current_tkl_humidity_timestamp = 0

current_ssh_temp_timestamp = 0
current_ssh_humidity_timestamp = 0

current_sek_temp_timestamp = 0
current_sek_humidity_timestamp = 0

current_pen_temp_timestamp = 0
current_pen_humidity_timestamp = 0

current_lfs_temp_timestamp = 0
current_lfs_humidity_timestamp = 0

current_ksc_temp_timestamp = 0
current_ksc_humidity_timestamp = 0

current_cch_temp_timestamp = 0
current_cch_humidity_timestamp = 0

current_clk_temp_timestamp = 0
current_clk_humidity_timestamp = 0

def get_time_str(current_temp_timestamp):
    current_temp_timestamp_str = str(current_temp_timestamp)
    year = current_temp_timestamp_str[:4]
    month = current_temp_timestamp_str[4:6]
    day = current_temp_timestamp_str[6:8]
    time_hour = current_temp_timestamp_str[8:10]
    if(int(time_hour) < 10):
        time_hour = time_hour[1]
    time_mins = current_temp_timestamp_str[10:12]
    date_time = year + '/' + month + '/' + day + ' ' + time_hour + ':' + time_mins
    return date_time


while True:
    changed_hko_temperature = 0
    changed_hko_humidity = 0

    changed_wlp_temperature = 0
    changed_wlp_humidity = 0

    changed_hks_temperature = 0
    changed_hks_humidity = 0

    changed_kp_temperature = 0
    changed_kp_humidity = 0

    changed_sha_temperature = 0
    changed_sha_humidity = 0

    changed_skg_temperature = 0
    changed_skg_humidity = 0

    changed_wgl_temperature = 0
    changed_wgl_humidity = 0

    changed_tun_temperature = 0
    changed_tun_humidity = 0

    changed_tw_temperature = 0
    changed_tw_humidity = 0

    changed_twn_temperature = 0
    changed_twn_humidity = 0

    changed_ty1_temperature = 0
    changed_ty1_humidity = 0

    changed_jkb_temperature = 0
    changed_jkb_humidity = 0

    changed_tpo_temperature = 0
    changed_tpo_humidity = 0

    changed_tkl_temperature = 0
    changed_tkl_humidity = 0

    changed_ssh_temperature = 0
    changed_ssh_humidity = 0

    changed_sek_temperature = 0
    changed_sek_humidity = 0

    changed_pen_temperature = 0
    changed_pen_humidity = 0

    changed_lfs_temperature = 0
    changed_lfs_humidity = 0

    changed_ksc_temperature = 0
    changed_ksc_humidity = 0

    changed_cch_temperature = 0
    changed_cch_humidity = 0

    changed_clk_temperature = 0
    changed_clk_humidity = 0

    temperature_data = requests.get(r'https://data.weather.gov.hk/weatherAPI/hko_data/regional-weather/latest_1min_temperature.csv').content
    temperature_df = pd.read_csv(io.StringIO(temperature_data.decode('utf-8')))
    humidity_data = requests.get(r'https://data.weather.gov.hk/weatherAPI/hko_data/regional-weather/latest_1min_humidity.csv').content
    humidity_df = pd.read_csv(io.StringIO(humidity_data.decode('utf-8')))

    if temperature_df.iloc[1, 0] != current_hko_temp_timestamp:
        hko_new_temp = temperature_df.loc[temperature_df['Automatic Weather Station'] == 'HK Observatory'].iloc[0, 2]
        current_hko_temp_timestamp = temperature_df.iloc[1, 0]
        changed_hko_temperature = 1
    
    if humidity_df.iloc[1, 0] != current_hko_humidity_timestamp:
        hko_new_humidity = humidity_df.loc[humidity_df['Automatic Weather Station'] == 'HK Observatory'].iloc[0,2]
        current_hko_humidity_timestamp = humidity_df.iloc[1, 0]
        changed_hko_humidity = 1

    if temperature_df.iloc[1, 0] != current_wlp_temp_timestamp:
        wlp_new_temp = temperature_df.loc[temperature_df['Automatic Weather Station'] == 'Wetland Park'].iloc[0, 2]
        current_wlp_temp_timestamp = temperature_df.iloc[1, 0]
        changed_wlp_temperature = 1

    if humidity_df.iloc[1, 0] != current_wlp_humidity_timestamp:
        wlp_new_humidity = humidity_df.loc[humidity_df['Automatic Weather Station'] == 'Wetland Park'].iloc[0, 2]
        current_wlp_humidity_timestamp = humidity_df.iloc[1, 0]
        changed_wlp_humidity = 1

    if temperature_df.iloc[1, 0] != current_hks_temp_timestamp:
        hks_new_temp = temperature_df.loc[temperature_df['Automatic Weather Station'] == 'Wong Chuk Hang'].iloc[0, 2]
        current_hks_temp_timestamp = temperature_df.iloc[1, 0]
        changed_hks_temperature = 1

    if humidity_df.iloc[1, 0] != current_hks_humidity_timestamp:
        hks_new_humidity = humidity_df.loc[humidity_df['Automatic Weather Station'] == 'Wong Chuk Hang'].iloc[0, 2]
        current_hks_humidity_timestamp = humidity_df.iloc[1, 0]
        changed_hks_humidity = 1

    if temperature_df.iloc[1, 0] != current_kp_temp_timestamp:
        kp_new_temp = temperature_df.loc[temperature_df['Automatic Weather Station'] == 'King\'s Park'].iloc[0, 2]
        current_kp_temp_timestamp = temperature_df.iloc[1, 0]
        changed_kp_temperature = 1 

    if humidity_df.iloc[1, 0] != current_kp_humidity_timestamp:
        kp_new_humidity = humidity_df.loc[humidity_df['Automatic Weather Station'] == 'King\'s Park'].iloc[0, 2]
        current_kp_humidity_timestamp = humidity_df.iloc[1, 0]
        changed_kp_humidity = 1

    if temperature_df.iloc[1, 0] != current_sha_temp_timestamp:
        sha_new_temp = temperature_df.loc[temperature_df['Automatic Weather Station'] == 'Sha Tin'].iloc[0, 2]
        current_sha_temp_timestamp = temperature_df.iloc[1, 0]
        changed_sha_temperature = 1 

    if humidity_df.iloc[1, 0] != current_sha_humidity_timestamp:
        sha_new_humidity = humidity_df.loc[humidity_df['Automatic Weather Station'] == 'Sha Tin'].iloc[0, 2]
        current_sha_humidity_timestamp = humidity_df.iloc[1, 0]
        changed_sha_humidity = 1

    if temperature_df.iloc[1, 0] != current_skg_temp_timestamp:
        skg_new_temp = temperature_df.loc[temperature_df['Automatic Weather Station'] == 'Sai Kung'].iloc[0, 2]
        current_skg_temp_timestamp = temperature_df.iloc[1, 0]
        changed_skg_temperature = 1 

    if humidity_df.iloc[1, 0] != current_skg_humidity_timestamp:
        skg_new_humidity = humidity_df.loc[humidity_df['Automatic Weather Station'] == 'Sai Kung'].iloc[0, 2]
        current_skg_humidity_timestamp = humidity_df.iloc[1, 0]
        changed_skg_humidity = 1

    if temperature_df.iloc[1, 0] != current_wgl_temp_timestamp:
        wgl_new_temp = temperature_df.loc[temperature_df['Automatic Weather Station'] == 'Waglan Island'].iloc[0, 2]
        current_wgl_temp_timestamp = temperature_df.iloc[1, 0]
        changed_wgl_temperature = 1 

    if humidity_df.iloc[1, 0] != current_wgl_humidity_timestamp:
        wgl_new_humidity = humidity_df.loc[humidity_df['Automatic Weather Station'] == 'Waglan Island'].iloc[0, 2]
        current_wgl_humidity_timestamp = humidity_df.iloc[1, 0]
        changed_wgl_humidity = 1    

    if temperature_df.iloc[1, 0] != current_tun_temp_timestamp:
        tun_new_temp = temperature_df.loc[temperature_df['Automatic Weather Station'] == 'Tuen Mun'].iloc[0, 2]
        current_tun_temp_timestamp = temperature_df.iloc[1, 0]
        changed_tun_temperature = 1 

    if humidity_df.iloc[1, 0] != current_tun_humidity_timestamp:
        tun_new_humidity = humidity_df.loc[humidity_df['Automatic Weather Station'] == 'Tuen Mun'].iloc[0, 2]
        current_tun_humidity_timestamp = humidity_df.iloc[1, 0]
        changed_tun_humidity = 1  

    if temperature_df.iloc[1, 0] != current_tw_temp_timestamp:
        tw_new_temp = temperature_df.loc[temperature_df['Automatic Weather Station'] == 'Tsuen Wan Shing Mun Valley'].iloc[0, 2]
        current_tw_temp_timestamp = temperature_df.iloc[1, 0]
        changed_tw_temperature = 1 

    if humidity_df.iloc[1, 0] != current_tw_humidity_timestamp:
        tw_new_humidity = humidity_df.loc[humidity_df['Automatic Weather Station'] == 'Tsuen Wan Shing Mun Valley'].iloc[0, 2]
        current_tw_humidity_timestamp = humidity_df.iloc[1, 0]
        changed_tw_humidity = 1

    if temperature_df.iloc[1, 0] != current_twn_temp_timestamp:
        twn_new_temp = temperature_df.loc[temperature_df['Automatic Weather Station'] == 'Tsuen Wan Ho Koon'].iloc[0, 2]
        current_twn_temp_timestamp = temperature_df.iloc[1, 0]
        changed_twn_temperature = 1 

    if humidity_df.iloc[1, 0] != current_twn_humidity_timestamp:
        twn_new_humidity = humidity_df.loc[humidity_df['Automatic Weather Station'] == 'Tsuen Wan Ho Koon'].iloc[0, 2]
        current_twn_humidity_timestamp = humidity_df.iloc[1, 0]
        changed_twn_humidity = 1

    if temperature_df.iloc[1, 0] != current_ty1_temp_timestamp:
        ty1_new_temp = temperature_df.loc[temperature_df['Automatic Weather Station'] == 'Tsing Yi'].iloc[0, 2]
        current_ty1_temp_timestamp = temperature_df.iloc[1, 0]
        changed_ty1_temperature = 1 

    if humidity_df.iloc[1, 0] != current_ty1_humidity_timestamp:
        ty1_new_humidity = humidity_df.loc[humidity_df['Automatic Weather Station'] == 'Tsing Yi'].iloc[0, 2]
        current_ty1_humidity_timestamp = humidity_df.iloc[1, 0]
        changed_ty1_humidity = 1

    if temperature_df.iloc[1, 0] != current_jkb_temp_timestamp:
        jkb_new_temp = temperature_df.loc[temperature_df['Automatic Weather Station'] == 'Tseung Kwan O'].iloc[0, 2]
        current_jkb_temp_timestamp = temperature_df.iloc[1, 0]
        changed_jkb_temperature = 1 

    if humidity_df.iloc[1, 0] != current_jkb_humidity_timestamp:
        jkb_new_humidity = humidity_df.loc[humidity_df['Automatic Weather Station'] == 'Tseung Kwan O'].iloc[0, 2]
        current_jkb_humidity_timestamp = humidity_df.iloc[1, 0]
        changed_jkb_humidity = 1

    if temperature_df.iloc[1, 0] != current_tpo_temp_timestamp:
        tpo_new_temp = temperature_df.loc[temperature_df['Automatic Weather Station'] == 'Tai Po'].iloc[0, 2]
        current_tpo_temp_timestamp = temperature_df.iloc[1, 0]
        changed_tpo_temperature = 1 

    if humidity_df.iloc[1, 0] != current_tpo_humidity_timestamp:
        tpo_new_humidity = humidity_df.loc[humidity_df['Automatic Weather Station'] == 'Tai Po'].iloc[0, 2]
        current_tpo_humidity_timestamp = humidity_df.iloc[1, 0]
        changed_tpo_humidity = 1

    if temperature_df.iloc[1, 0] != current_tkl_temp_timestamp:
        tkl_new_temp = temperature_df.loc[temperature_df['Automatic Weather Station'] == 'Ta Kwu Ling'].iloc[0, 2]
        current_tkl_temp_timestamp = temperature_df.iloc[1, 0]
        changed_tkl_temperature = 1 

    if humidity_df.iloc[1, 0] != current_tkl_humidity_timestamp:
        tkl_new_humidity = humidity_df.loc[humidity_df['Automatic Weather Station'] == 'Ta Kwu Ling'].iloc[0, 2]
        current_tkl_humidity_timestamp = humidity_df.iloc[1, 0]
        changed_tkl_humidity = 1

    if temperature_df.iloc[1, 0] != current_ssh_temp_timestamp:
        ssh_new_temp = temperature_df.loc[temperature_df['Automatic Weather Station'] == 'Sheung Shui'].iloc[0, 2]
        current_ssh_temp_timestamp = temperature_df.iloc[1, 0]
        changed_ssh_temperature = 1 

    if humidity_df.iloc[1, 0] != current_ssh_humidity_timestamp:
        ssh_new_humidity = humidity_df.loc[humidity_df['Automatic Weather Station'] == 'Sheung Shui'].iloc[0, 2]
        current_ssh_humidity_timestamp = humidity_df.iloc[1, 0]
        changed_ssh_humidity = 1

    if temperature_df.iloc[1, 0] != current_sek_temp_timestamp:
        sek_new_temp = temperature_df.loc[temperature_df['Automatic Weather Station'] == 'Shek Kong'].iloc[0, 2]
        current_sek_temp_timestamp = temperature_df.iloc[1, 0]
        changed_sek_temperature = 1 

    if humidity_df.iloc[1, 0] != current_sek_humidity_timestamp:
        sek_new_humidity = humidity_df.loc[humidity_df['Automatic Weather Station'] == 'Shek Kong'].iloc[0, 2]
        current_sek_humidity_timestamp = humidity_df.iloc[1, 0]
        changed_sek_humidity = 1

    if temperature_df.iloc[1, 0] != current_pen_temp_timestamp:
        pen_new_temp = temperature_df.loc[temperature_df['Automatic Weather Station'] == 'Peng Chau'].iloc[0, 2]
        current_pen_temp_timestamp = temperature_df.iloc[1, 0]
        changed_pen_temperature = 1 

    if humidity_df.iloc[1, 0] != current_pen_humidity_timestamp:
        pen_new_humidity = humidity_df.loc[humidity_df['Automatic Weather Station'] == 'Peng Chau'].iloc[0, 2]
        current_pen_humidity_timestamp = humidity_df.iloc[1, 0]
        changed_pen_humidity = 1

    if temperature_df.iloc[1, 0] != current_lfs_temp_timestamp:
        lfs_new_temp = temperature_df.loc[temperature_df['Automatic Weather Station'] == 'Lau Fau Shan'].iloc[0, 2]
        current_lfs_temp_timestamp = temperature_df.iloc[1, 0]
        changed_lfs_temperature = 1 

    if humidity_df.iloc[1, 0] != current_lfs_humidity_timestamp:
        lfs_new_humidity = humidity_df.loc[humidity_df['Automatic Weather Station'] == 'Lau Fau Shan'].iloc[0, 2]
        current_lfs_humidity_timestamp = humidity_df.iloc[1, 0]
        changed_lfs_humidity = 1

    if temperature_df.iloc[1, 0] != current_ksc_temp_timestamp:
        ksc_new_temp = temperature_df.loc[temperature_df['Automatic Weather Station'] == 'Kau Sai Chau'].iloc[0, 2]
        current_ksc_temp_timestamp = temperature_df.iloc[1, 0]
        changed_ksc_temperature = 1 

    if humidity_df.iloc[1, 0] != current_ksc_humidity_timestamp:
        ksc_new_humidity = humidity_df.loc[humidity_df['Automatic Weather Station'] == 'Kau Sai Chau'].iloc[0, 2]
        current_ksc_humidity_timestamp = humidity_df.iloc[1, 0]
        changed_ksc_humidity = 1

    if temperature_df.iloc[1, 0] != current_cch_temp_timestamp:
        cch_new_temp = temperature_df.loc[temperature_df['Automatic Weather Station'] == 'Cheung Chau'].iloc[0, 2]
        current_cch_temp_timestamp = temperature_df.iloc[1, 0]
        changed_cch_temperature = 1 

    if humidity_df.iloc[1, 0] != current_cch_humidity_timestamp:
        cch_new_humidity = humidity_df.loc[humidity_df['Automatic Weather Station'] == 'Cheung Chau'].iloc[0, 2]
        current_cch_humidity_timestamp = humidity_df.iloc[1, 0]
        changed_cch_humidity = 1

    if temperature_df.iloc[1, 0] != current_clk_temp_timestamp:
        clk_new_temp = temperature_df.loc[temperature_df['Automatic Weather Station'] == 'Chek Lap Kok'].iloc[0, 2]
        current_clk_temp_timestamp = temperature_df.iloc[1, 0]
        changed_clk_temperature = 1 

    if humidity_df.iloc[1, 0] != current_clk_humidity_timestamp:
        clk_new_humidity = humidity_df.loc[humidity_df['Automatic Weather Station'] == 'Chek Lap Kok'].iloc[0, 2]
        current_clk_humidity_timestamp = humidity_df.iloc[1, 0]
        changed_clk_humidity = 1


    if current_hko_temp_timestamp == current_hko_humidity_timestamp and changed_hko_temperature == 1 and changed_hko_humidity == 1:
        date_time = get_time_str(current_hko_temp_timestamp)
        new_row_df = pd.DataFrame([date_time, hko_new_temp, hko_new_humidity])
        new_row_df.T.to_csv('data/hko_update_weather.csv', mode='a', header=False, index=False)

    if current_wlp_temp_timestamp == current_wlp_humidity_timestamp and changed_wlp_temperature == 1 and changed_wlp_humidity == 1:
        date_time = get_time_str(current_wlp_temp_timestamp)
        new_row_df = pd.DataFrame([date_time, wlp_new_temp, wlp_new_humidity])
        new_row_df.T.to_csv('data/wlp_update_weather.csv', mode='a', header=False, index=False)

    if current_hks_temp_timestamp == current_hks_humidity_timestamp and changed_hks_temperature == 1 and changed_hks_humidity == 1:
        date_time = get_time_str(current_hks_temp_timestamp)
        new_row_df = pd.DataFrame([date_time, hks_new_temp, hks_new_humidity])
        new_row_df.T.to_csv('data/hks_update_weather.csv', mode='a', header=False, index=False)
    
    if current_kp_temp_timestamp == current_kp_humidity_timestamp and changed_kp_temperature == 1 and changed_kp_humidity == 1:
        date_time = get_time_str(current_kp_temp_timestamp)
        new_row_df = pd.DataFrame([date_time, kp_new_temp, kp_new_humidity])
        new_row_df.T.to_csv('data/kp_update_weather.csv', mode='a', header=False, index=False)

    if current_sha_temp_timestamp == current_sha_humidity_timestamp and changed_sha_temperature == 1 and changed_sha_humidity == 1:
        data_time = get_time_str(current_sha_temp_timestamp)
        new_row_df = pd.DataFrame([date_time, sha_new_temp, sha_new_humidity])
        new_row_df.T.to_csv('data/sha_update_weather.csv', mode='a', header=False, index=False)

    if current_skg_temp_timestamp == current_skg_humidity_timestamp and changed_skg_temperature == 1 and changed_skg_humidity == 1:
        date_time = get_time_str(current_skg_temp_timestamp)
        new_row_df = pd.DataFrame([date_time, skg_new_temp, skg_new_humidity])
        new_row_df.T.to_csv('data/skg_update_weather.csv', mode='a', header=False, index=False)
    
    if current_wgl_temp_timestamp == current_wgl_humidity_timestamp and changed_wgl_temperature == 1 and changed_wgl_humidity == 1:
        date_time = get_time_str(current_wgl_temp_timestamp)
        new_row_df = pd.DataFrame([date_time, wgl_new_temp, wgl_new_humidity])
        new_row_df.T.to_csv('data/wgl_update_weather.csv', mode='a', header=False, index=False)

    if current_tun_temp_timestamp == current_tun_humidity_timestamp and changed_tun_temperature == 1 and changed_tun_humidity == 1:
        date_time = get_time_str(current_tun_temp_timestamp)
        new_row_df = pd.DataFrame([date_time, tun_new_temp, tun_new_humidity])
        new_row_df.T.to_csv('data/tun_update_weather.csv', mode='a', header=False, index=False)

    if current_tw_temp_timestamp == current_tw_humidity_timestamp and changed_tw_temperature == 1 and changed_tw_humidity == 1:
        date_time = get_time_str(current_tw_temp_timestamp)
        new_row_df = pd.DataFrame([date_time, tw_new_temp, tw_new_humidity])
        new_row_df.T.to_csv('data/tw_update_weather.csv', mode='a', header=False, index=False)

    if current_twn_temp_timestamp == current_twn_humidity_timestamp and changed_twn_temperature == 1 and changed_twn_humidity == 1:
        date_time = get_time_str(current_twn_temp_timestamp)
        new_row_df = pd.DataFrame([date_time, twn_new_temp, twn_new_humidity])
        new_row_df.T.to_csv('data/twn_update_weather.csv', mode='a', header=False, index=False)

    if current_ty1_temp_timestamp == current_ty1_humidity_timestamp and changed_ty1_temperature == 1 and changed_ty1_humidity == 1:
        date_time = get_time_str(current_ty1_temp_timestamp)
        new_row_df = pd.DataFrame([date_time, ty1_new_temp, ty1_new_humidity])
        new_row_df.T.to_csv('data/ty1_update_weather.csv', mode='a', header=False, index=False)

    if current_jkb_temp_timestamp == current_jkb_humidity_timestamp and changed_jkb_temperature == 1 and changed_jkb_humidity == 1:
        date_time = get_time_str(current_jkb_temp_timestamp)
        new_row_df = pd.DataFrame([date_time, jkb_new_temp, jkb_new_humidity])
        new_row_df.T.to_csv('data/jkb_update_weather.csv', mode='a', header=False, index=False)

    if current_tpo_temp_timestamp == current_tpo_humidity_timestamp and changed_tpo_temperature == 1 and changed_tpo_humidity == 1:
        data_time = get_time_str(current_tpo_temp_timestamp)
        new_row_df = pd.DataFrame([data_time, tpo_new_temp, tpo_new_humidity])
        new_row_df.T.to_csv('data/tpo_update_weather.csv', mode='a', header=False, index=False)

    if current_tkl_temp_timestamp == current_tkl_humidity_timestamp and changed_tkl_temperature == 1 and changed_tkl_humidity == 1:
        data_time = get_time_str(current_tkl_temp_timestamp)
        new_row_df = pd.DataFrame([data_time, tkl_new_temp, tkl_new_humidity])
        new_row_df.T.to_csv('data/tkl_update_weather.csv', mode='a', header=False, index=False)

    if current_ssh_temp_timestamp == current_ssh_humidity_timestamp and changed_ssh_temperature == 1 and changed_ssh_humidity == 1:
        data_time = get_time_str(current_ssh_temp_timestamp)
        new_row_df = pd.DataFrame([data_time, ssh_new_temp, ssh_new_humidity])
        new_row_df.T.to_csv('data/ssh_update_weather.csv', mode='a', header=False, index=False)

    if current_sek_temp_timestamp == current_sek_humidity_timestamp and changed_sek_temperature == 1 and changed_sek_humidity == 1:
        date_time = get_time_str(current_sek_temp_timestamp)
        new_row_df = pd.DataFrame([date_time, ssh_new_temp, ssh_new_humidity])
        new_row_df.T.to_csv('data/sek_update_weather.csv', mode='a', header=False, index=False)

    if current_pen_temp_timestamp == current_pen_humidity_timestamp and changed_pen_temperature == 1 and changed_pen_humidity == 1:
        date_time = get_time_str(current_pen_temp_timestamp)
        new_row_df = pd.DataFrame([date_time, pen_new_temp, pen_new_humidity])
        new_row_df.T.to_csv('data/pen_update_weather.csv', mode='a', header=False, index=False)

    if current_lfs_temp_timestamp == current_lfs_humidity_timestamp and changed_lfs_temperature == 1 and changed_lfs_humidity == 1:
        date_time = get_time_str(current_lfs_temp_timestamp)
        new_row_df = pd.DataFrame([data_time, lfs_new_temp, lfs_new_humidity])
        new_row_df.T.to_csv('data/lfs_update_weather.csv', mode='a', header=False, index=False)

    if current_ksc_temp_timestamp == current_ksc_humidity_timestamp and changed_ksc_temperature == 1 and changed_ksc_humidity == 1:
        date_time = get_time_str(current_ksc_temp_timestamp)
        new_row_df = pd.DataFrame([data_time, ksc_new_temp, ksc_new_humidity])
        new_row_df.T.to_csv('data/ksc_update_weather.csv', mode='a', header=False, index=False)
    
    if current_cch_temp_timestamp == current_cch_humidity_timestamp and changed_cch_temperature == 1 and changed_cch_humidity == 1:
        date_time = get_time_str(current_cch_temp_timestamp)
        new_row_df = pd.DataFrame([date_time, cch_new_temp, cch_new_humidity])
        new_row_df.T.to_csv('data/cch_update_weather.csv', mode='a', header=False, index=False)

    if current_clk_temp_timestamp == current_clk_humidity_timestamp and changed_clk_temperature == 1 and changed_clk_humidity == 1:
        date_time = get_time_str(current_clk_temp_timestamp)
        new_row_df = pd.DataFrame([data_time, clk_new_temp, clk_new_humidity])
        new_row_df.T.to_csv('data/clk_update_weather.csv', mode='a', header=False, index=False)

    print('hello')
    time.sleep(600)
    
    