from datetime import datetime, timedelta
from pandas import date_range


#-------------------------------------------------------------------------------------------
import weather_ilens
import weather_format
import weather_missing
#-------------------------------------------------------------------------------------------


def past():
    #call weather_ilens to download the missing data from ilens
    time_period = date_range(start = '2016-12-08', end = datetime.today() - timedelta(days = 1), freq = '1D')
    weather_ilens.ilens(time_period, past=True)

    #format the file to suit the other files from HKO
    weather_format.format(past=True)


if __name__=='__main__':
    past()
