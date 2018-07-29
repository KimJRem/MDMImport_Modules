import os.path
import csv
import numpy as np
import pandas as pd
import pika
import json
from flatten_json import flatten
from logging_one import *
from ConsumeRabbitMQ import *
from PublishRabbitMQ import *
from AnalysisHelperClass import *


# https://stackoverflow.com/questions/50813108/get-transferred-file-name-in-rabbitmq-using-python-pika
# for transferring csv files

class TrafficRenameColumns:
    setup_logging()

    # do statistical operation
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
        # decode csv file
        data = analysisTask.decodeCsv(csv_data)
        # turn csv file into DF
        df = analysisTask.csvToDF(data)
        print('Print DF: ')
        print(df)
        # rename
        task = TrafficRenameColumns()
        renamedDF = task.renameColumns(df)
        print('Print renamed DF: ')
        print(renamedDF)


        #it is not working yet converting DF to Json to send via RabbitMQ,
        #then the other classes do not need to import this class but can use input can be
        #send via RabbitMQ

        #data cannot be sent to RabbitM
        #routingPublish = 'ef'
        #pushRabbitMDM = PublishRabbitMQ()
        #pushRabbitMDM.startImport(toJson, routingPublish)
        #print('Dict was pushed to queue')

        # if we want to send the result to somewhere for using
        # then convert to dict and send dict, at consumer reverse again

main()
