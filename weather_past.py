from datetime import datetime, timedelta
from pandas import date_range


#-------------------------------------------------------------------------------------------
#import weather_ilens
#import weather_format
#import weather_missing
#call other scripts
import importlib.util
import sys

spec = importlib.util.spec_from_file_location("format", "D:/test_development/Weather/Weather_sort/weather_format.py")
weather_format = importlib.util.module_from_spec(spec)
sys.modules["format"] = weather_format
spec.loader.exec_module(weather_format)

spec = importlib.util.spec_from_file_location("ilens", "D:/test_development/Weather/Weather_down/weather_ilens.py")
weather_ilens = importlib.util.module_from_spec(spec)
sys.modules["ilens"] = weather_ilens
spec.loader.exec_module(weather_ilens)

spec = importlib.util.spec_from_file_location("check", "D:/test_development/Weather/Weather_missing/weather_missing.py")
weather_missing = importlib.util.module_from_spec(spec)
sys.modules["check"] = weather_missing
spec.loader.exec_module(weather_missing)

spec = importlib.util.spec_from_file_location("combine", "D:/test_development/Weather/Weather_missing/weather_missing.py")
weather_missing = importlib.util.module_from_spec(spec)
sys.modules["combine"] = weather_missing
spec.loader.exec_module(weather_missing)
#-------------------------------------------------------------------------------------------


def past():
    #call weather_ilens to download the missing data from ilens
    time_period = date_range(start = '2016-12-08', end = datetime.today() - timedelta(days = 1), freq = '1D')
    #weather_ilens.ilens(time_period, past=True)

    #format the file to suit the other files from HKO
    weather_format.format(past=True)

    #return all the missing time(not necessary for now)
    #missing = weather_missing.all_missing(time_period)


if __name__=='__main__':
    past()