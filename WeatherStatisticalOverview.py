import datetime
import os.path
import csv
import numpy as np
import pandas as pd
import pika
from flatten_json import flatten
from logging_one import *
from ConsumeRabbitMQ import *
from PublishRabbitMQ import *
from AnalysisHelperClass import *


# https://stackoverflow.com/questions/50813108/get-transferred-file-name-in-rabbitmq-using-python-pika
# for transferring csv files

class WeatherStatisticalOverview:
    setup_logging()

    # select important columns, rename columns, convert Kelvin to Celsius, convert Date to readable format
    def weather_API_dataPrep(self, df):
        # extract only relevant columns
        df_selectedColumns = df[
            ['dt', 'weather_0_main', 'weather_0_description', 'main_temp', 'main_temp_min', 'main_temp_max',
             'main_pressure', 'main_humidity', 'sys_sunrise', 'sys_sunset', 'wind_speed', 'wind_deg',
             'clouds_all', 'visibility', 'rain_3h', 'snow_3h']]
        # rename columns
        df_selectedColumns = df_selectedColumns.rename(
            columns={"dt": "Date_unix", "weather_0_main": "General_description",
                     "weather_0_description": "Short_description",
                     "main_temp": "Temperature", "main_temp_min": "Min_Temperature",
                     "main_temp_max": "Max_Temperature", "main_pressure": "Pressure",
                     "main_humidity": "Humidity", "sys_sunrise": "Sunrise",
                     "sys_sunset": "Sunset", "wind_speed": "Wind_speed",
                     "wind_deg": "Wind_direction", "clouds_all": "Clouds",
                     "visibility": "Visibility", "rain_3h": "Rain_last3h", "snow_3h": "Snow_last3h"})
        # create three new columns with °C instead of Kelvin, alternatively get Celsius directly from API
        df_selectedColumns['Temp_Celsius'] = (df_selectedColumns.Temperature - 273.15)
        df_selectedColumns['Min_Temperature_Celsius'] = (df_selectedColumns.Min_Temperature - 273.15)
        df_selectedColumns['Max_Temperature_Celsius'] = (df_selectedColumns.Max_Temperature - 273.15)
        # convert date to readable format, should be the same format as the MDM data
        df_selectedColumns['Date'] = df_selectedColumns.apply(
            lambda row: datetime.datetime.utcfromtimestamp(row['Date_unix']).replace(tzinfo=datetime.timezone.utc),
            axis=1)
        df_selectedColumns['Date'] = pd.to_datetime(df_selectedColumns['Date']).dt.tz_convert('Europe/Berlin')
        return df_selectedColumns

    # Statistical overview of all columns
    def ALL_describe(self, df):
        stats = df.describe()
        print('Statistical overview of the weather data: ')
        return stats

    # Statistical overview of selected columns
    def describe(self, df, column_name):
        stats = df[column_name].describe()
        print('Statistical overview of the column %s: ' % (column_name))
        return stats


# Problem da csv nicht nur einmal, sondern 12x gesendet wird. Wird das hier auch 12x gemacht.
def main():
    while True:
        logger = logging.getLogger(__name__)
        # consume from Queue
        routingConsume = '24'
        consumeRabbitMDM = ConsumeRabbitMQ()
        logger.info('First')
        csv_data = consumeRabbitMDM.startConsuming(routingConsume)
        logger.info('Second')
        print(csv_data)
        logger.info('Third')

        analysisTask = AnalysisHelperClass()
        # decode csv file
        data = analysisTask.decodeCsv(csv_data)
        # turn csv file into DF
        df = analysisTask.csvToDF(data)
        print('Print DF: ')
        print(df)
        # do statistical overview
        task = WeatherStatisticalOverview()
        data_Prep_DF = task.weather_API_dataPrep(df)
        print(data_Prep_DF)

        stats = task.ALL_describe(data_Prep_DF)
        print(stats)
        column_stats = task.describe(data_Prep_DF, 'Temp_Celsius')
        print(column_stats)

        # if we want to send the result to somewhere for using
        # then convert to dict and send dict, at consumer reverse again


main()
