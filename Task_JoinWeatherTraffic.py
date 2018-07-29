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

class Task_JoinWeatherTraffic:
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
        #create three new columns with °C instead of Kelvin, alternatively get Celsius directly from API
        df_selectedColumns['Temp_Celsius'] = (df_selectedColumns.Temperature - 273.15)
        df_selectedColumns['Min_Temperature_Celsius'] = (df_selectedColumns.Min_Temperature - 273.15)
        df_selectedColumns['Max_Temperature_Celsius'] = (df_selectedColumns.Max_Temperature - 273.15)
        # convert date to readable format, should be the same format as the MDM data
        df_selectedColumns['Date'] = df_selectedColumns.apply(
            lambda row: datetime.datetime.utcfromtimestamp(row['Date_unix']).replace(tzinfo=datetime.timezone.utc),
            axis=1)
        # set Date to one conform standard
        df_selectedColumns['Date'] = pd.to_datetime(df_selectedColumns['Date']).dt.tz_convert('Europe/Berlin')
        return df_selectedColumns

    # dataPrepTraffic = rename
    def traffic_dataPrep(self, dataframe):
        columnNames = list(dataframe.head(0))
        firstColumnName = columnNames[0]

        if firstColumnName == 'parkingAreaOccupancy':
            dfArea = dataframe.rename(
                columns={"parkingAreaOccupancy": "OccupancyRate", "parkingAreaReference_@targetClass": "TargetClass",
                         "parkingAreaReference_@id": "ParkingAreaID", "parkingAreaReference_@version": "Version",
                         "parkingAreaStatusTime": "Date",
                         "parkingAreaTotalNumberOfVacantParkingSpaces": "TotalNumberOfVacantParkingSpaces",
                         "totalParkingCapacityLongTermOverride": "ParkingCapacityLongTerm",
                         "totalParkingCapacityShortTermOverride": "ParkingCapacityShortTerm"})
            dfArea['Date'] = pd.to_datetime(dfArea['Date']).dt.tz_localize('UTC').dt.tz_convert('Europe/Berlin')
            return dfArea
        else:
            dfFacility = dataframe.rename(
                columns={"parkingFacilityOccupancy": "OccupancyRateFacility",
                         "parkingFacilityReference_@targetClass": "TargetClass",
                         "parkingFacilityReference_@id": "ParkingFacilityID",
                         "parkingFacilityReference_@version": "Version",
                         "parkingFacilityStatus": "Status", "parkingFacilityStatusTime": "Date",
                         "totalNumberOfOccupiedParkingSpaces": "TotalNumberOfOccupiedParkingSpaces",
                         "totalNumberOfVacantParkingSpaces": "TotalNumberOfVacantParkingSpaces",
                         "totalParkingCapacityOverride": "ParkingCapacity",
                         "totalParkingCapacityShortTermOverride": "ParkingCapacityShortTerm"})
            dfFacility['Date'] = pd.to_datetime(dfFacility['Date']).dt.tz_localize('UTC').dt.tz_convert('Europe/Berlin')
            return dfFacility

    # merge dataset on Time (+/- 2 minutes)
    # drop all rows where column "Short_description" has NaN values, assuming "Short_description" will be in every import from the weather API
    def mergeDatasets(self, dfWeather, dfTraffic):
        newDF = pd.merge_asof(dfWeather, dfTraffic, on='Date', tolerance=pd.Timedelta('12000ms'))
        newDF.to_csv('verkehrundwetter.csv', sep='\t', encoding='utf-8')
        print(newDF)
        return newDF

    def mergeDatasetsNew(self, dfWeather, dfTraffic):
        columnNames = list(dfTraffic.head(0))
        firstColumnName = columnNames[0]
        if firstColumnName == 'parkingAreaOccupancy':
            newDFArea = pd.merge_asof(dfWeather, dfTraffic, on='Date', tolerance=pd.Timedelta('12000ms'))
            b = newDFArea.dropna(subset=['Short_description'])
            b.to_csv('areaundwetter.csv', sep='\t', encoding='utf-8')
            print(newDFArea)
        else:
            newDFFacility = pd.merge_asof(dfWeather, dfTraffic, on='Date', tolerance=pd.Timedelta('12000ms'))
            c = newDFFacility.dropna(subset=['Short_description'])
            c.to_csv('facilityundwetter.csv', sep='\t', encoding='utf-8')
            print(newDFFacility)


# Problem da csv nicht nur einmal, sondern 12x gesendet wird. Wird das hier auch 12x gemacht.
def main():
    while True:
        logger = logging.getLogger(__name__)
        # consume from Queue
        routingConsumeWeather = '24'
        routingConsumeTraffic = 'cd'
        consumeRabbitMDM = ConsumeRabbitMQ()
        logger.info('First')
        csv_dataWeather = consumeRabbitMDM.startConsuming(routingConsumeWeather)
        logger.info('Second')
        print(csv_dataWeather)
        csv_Traffic = consumeRabbitMDM.startConsuming(routingConsumeTraffic)
        logger.info('Third')
        print(csv_Traffic)

        #weather data
        analysisTask = AnalysisHelperClass()
        dataWeather = analysisTask.decodeCsv(csv_dataWeather)
        dfWeather = analysisTask.csvToDF(dataWeather)
        print('Print DF: ')
        print(dfWeather)

        #traffic data
        analysisTask = AnalysisHelperClass()
        dataTraffic = analysisTask.decodeCsv(csv_Traffic)
        dfTraffic = analysisTask.csvToDF(dataTraffic)
        print('Print DF: ')
        print(dfTraffic)

        # do the join
        task = Task_JoinWeatherTraffic()
        weatherPrep_DF = task.weather_API_dataPrep(dfWeather)
        trafficPrep_DF = task.traffic_dataPrep(dfTraffic)

        task.mergeDatasetsNew(weatherPrep_DF, trafficPrep_DF)

        #if we want to do an analysis with the merged data, we need to push the new csv(s)
        #and then use those csv(s) in the analysis by consuming the csv

main()
