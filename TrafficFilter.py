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

class TrafficFilter:
    setup_logging()

    # dataPrep = rename, used directly, other solution import the class
    def renameColumns(self, dataframe):
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
            return dfFacility

    # check for most popular parking space by occupancy, works before and after rename function has been applied
    def popularParkingbyID(self, df):
        # make
        columnNames = list(df.head(0))
        firstColumnName = columnNames[0]
        if firstColumnName == 'parkingAreaOccupancy':
            df_filter = df[firstColumnName].groupby(df['parkingAreaReference_@id']).mean()
            i = df_filter.nlargest(4)
        elif firstColumnName == 'OccupancyRate':
            df_filter = df[firstColumnName].groupby(df['ParkingAreaID']).mean()
            i = df_filter.nlargest(4)
        elif firstColumnName == 'parkingFacilityOccupancy':
            df_filter = df[firstColumnName].groupby(df['parkingFacilityReference_@id']).mean()
            i = df_filter.nlargest(4)
        elif firstColumnName == 'OccupancyRateFacility':
            df_filter = df[firstColumnName].groupby(df['ParkingFacilityID']).mean()
            i = df_filter.nlargest(4)
        return i


# Problem da csv nicht nur einmal, sondern 12x gesendet wird. Wird das hier auch 12x gemacht.
def main():
    while True:
        logger = logging.getLogger(__name__)
        # consume from Queue
        routingConsume = 'cd'
        consumeRabbitMDM = ConsumeRabbitMQ()
        logger.info('First')
        csv_data = consumeRabbitMDM.startConsuming(routingConsume)
        logger.info('Second')
        print(csv_data)
        logger.info('Third')

        analysisTask = AnalysisHelperClass()

        if type(csv_data) == type(dict()):  # not working yet, if so only Json conversion is sensible
            df = analysisTask.DictToDF(csv_data)
        else:
            # decode csv file
            data = analysisTask.decodeCsv(csv_data)
            # turn csv file into DF
            df = analysisTask.csvToDF(data)
            print('Print DF: ')
            print(df)

        # do statistical overview
        task = TrafficFilter()
        renamed_DF = task.renameColumns(df)
        filter = task.popularParkingbyID(renamed_DF)
        print('Print filter: ')
        print(filter)
        print(filter.dtypes)

        # if we want to send the result to somewhere for using
        # then convert to dict and send dict, at consumer reverse again


main()
