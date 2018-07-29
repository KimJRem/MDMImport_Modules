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

#https://stackoverflow.com/questions/50813108/get-transferred-file-name-in-rabbitmq-using-python-pika
#for transferring csv files

class TrafficStatisticalOverview:

    setup_logging()

    # dataPrep = rename
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

    # do statistical operation
    def ALL_describe(self, df):
        #stats = df.describe()# make the code more flexible - automatically insert the name of the DF
        stats = df.describe(include=[np.number])
        print('Stats return type: ')
        print(stats.dtypes)
        return stats


#Problem da csv nicht nur einmal, sondern 12x gesendet wird. Wird das hier auch 12x gemacht.
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
        #decode csv file
        data = analysisTask.decodeCsv(csv_data)
        #turn csv file into DF
        df = analysisTask.csvToDF(data)
        print('Print DF: ')
        print(df)
        #do statistical overview
        task = TrafficStatisticalOverview()
        renamed_DF = task.renameColumns(df)
        stats = task.ALL_describe(renamed_DF)
        print(stats)

        #def DFtoJSON(self, df):

        # if we want to send the result to somewhere for using
        #then convert df to json and send json over the queue
main()