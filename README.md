# weather
overview:

First use weather_past to download the history data from i-lens.
It will call weather_down to do so.
Then it will call weather_sort for formatting.
Past data can be directly stored in the data file for the other data.

Secondly, we can execute the weather_update to keep updating the weather data.
Weather_missing will check if the data of the day before yesterday is missing.
If there is any data leakage, it will call weather_down to download it to missing_file.
Weather_sort will also take care of the format.
Then, weather_missing will check will combine the old data and the new data and store it in the data file.

execution:

execute waether_past.py to download the past data from i-lens.
execute weather_update.py to update the current data from HKO.
