import csv
import numpy as np
import pandas as pd
import json
from traffic.HelperClass import HelperClass
from traffic.RabbitMQConsumer import RabbitMQConsumer
from traffic.mdm_logging import *
from traffic.mdm_logging import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

# https://stackoverflow.com/questions/50813108/get-transferred-file-name-in-rabbitmq-using-python-pika
# for transferring csv files

class TrafficFilterLargestNew:

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
        columnNames = list(df.head(0))
        firstColumnName = columnNames[0]
        # if statements so it works without renameColumns() function too
        if firstColumnName == 'parkingAreaOccupancy':
            df_filter = df[firstColumnName].groupby(df['parkingAreaReference_@id']).mean()
            i = df_filter.nlargest(4)
        elif firstColumnName == 'OccupancyRate':
            df_filter = df[firstColumnName].groupby(df['ParkingAreaID']).mean()
            i = df_filter.nlargest(4)
        elif firstColumnName == 'parkingFacilityOccupancy':
            # take only those parkingFacilities with "open" status into consideration
            df = df[df.parkingFacilityStatus == 'open']
            df_filter = df[firstColumnName].groupby(df['parkingFacilityReference_@id']).mean()
            i = df_filter.nlargest(4)
        elif firstColumnName == 'OccupancyRateFacility':
            print(df)
            df = df[df.Status == 'open']
            print(df)
            df_filter = df[firstColumnName].groupby(df['ParkingFacilityID']).mean()
            i = df_filter.nlargest(4)
        return i


consumer_config = json.dumps({
    "exchangeName": "topic_datas",
    "host": "localhost",
    "routingKey": "cd",
    "exchangeType": "direct",
    "queueName": "cd",
    "exchangeOptions": {
        "passive": False,
        "durable": False,
        "autoDelete": False,
        "internal": False
    },
    "queueOptions": {
        "passive": False,
        "durable": False,
        "exclusive": False,
        "autoDelete": False
    }
})

def main():
    # consume from Queue
    with RabbitMQConsumer(json.loads(consumer_config)) as consumer:
        logger.info('Consume')
        consumer.consume(resolve_message)


def resolve_message(data):

    print(" [x] Receiving message %r" % data)

    analysisTask = HelperClass()

    # decode csv file
    decoded_csv = analysisTask.decodeCsv(data)
    # turn csv file into DF
    df = analysisTask.csvToDF(decoded_csv)
    logger.info('DF has been created')

    # do statistical overview
    task = TrafficFilterLargestNew()
    renamed_DF = task.renameColumns(df)
    filter = task.popularParkingbyID(renamed_DF)
    print('Print filter: ')
    print(filter)
    logger.info('Filter has been applied to the data set')

main()