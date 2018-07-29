import os.path
import csv
import pika
from flatten_json import flatten
from logging_one import *
from ConsumeRabbitMQ import *
from PublishRabbitMQ import *

#https://stackoverflow.com/questions/50813108/get-transferred-file-name-in-rabbitmq-using-python-pika
#for transferring csv files

class Task_JsonCsv:

    setup_logging()
    filenameArea = None
    filenameFacility = None
    k = 0

    def JSONtoCsvArea(self, **data):
        logger = logging.getLogger(__name__)
        #filename = 'mdm_data_parkingArea.csv'
        global filenameArea
        filenameArea = 'mdm_data_parkingArea.csv'
        with open(filenameArea, "a") as csvfile:
            fileEmpty = os.stat(filenameArea).st_size == 0
            headers = list(data)
            writer = csv.DictWriter(csvfile, delimiter=',', lineterminator='\n',fieldnames=headers)
            if fileEmpty:
                #print("Header is written now")
                logger.info('Header is written now')
                writer.writeheader()  # file doesn't exist yet, write a header
            writer.writerow(data)
            #print('Received one set of data')
            logger.info('Received one set of data')

    def JSONtoCsvFacility(self, **data):
        logger = logging.getLogger(__name__)
        #filename = 'mdm_data_parkingFacility.csv'
        global filenameFacility
        filenameFacility = 'mdm_data_parkingFacility.csv'
        with open(filenameFacility,"a") as csvfile:
            fileEmpty = os.stat(filenameFacility).st_size == 0
            headers = list(data)
            writer = csv.DictWriter(csvfile, delimiter=',', lineterminator='\n',fieldnames=headers)
            if fileEmpty:
                #print("Header is written now")
                logger.info('Header is written now')
                writer.writeheader()
            writer.writerow(data)
            #print('Received one set of data')
            logger.info('Received one set of data')

    def getCsv(self, filename):
        with open(filename, 'rb') as csv_file:
            return filename + csv_file.read().decode()

    def main(self):
        global k
        k = 0
        while True:
            logger = logging.getLogger(__name__)
            # consume from Queue
            routingConsume = 'bc'
            consumeRabbitMDM = ConsumeRabbitMQ()
            logger.info('First')
            global json_data
            json_data = consumeRabbitMDM.startConsuming(routingConsume)
            logger.info('Second')
            # print(json_data)
            logger.info('Third')
            taskOne = Task_JsonCsv()

            if k < 11:
                data = json.loads(json_data)
                x = flatten(data)
                firstkey = list(x.keys())[0]
                # taskOne = Task_JsonCsv()
                if firstkey == 'parkingAreaOccupancy':
                    taskOne.JSONtoCsvArea(**x)
                    logger.info('Stored data in csvArea')
                else:
                    taskOne.JSONtoCsvFacility(**x)
                    logger.info('Stored data in csvFacility')
                k = k + 1
                print(k)
            else:
                data = json.loads(json_data)
                x = flatten(data)
                firstkey = list(x.keys())[0]
                # taskOne = Task_JsonCsv()
                if firstkey == 'parkingAreaOccupancy':
                    taskOne.JSONtoCsvArea(**x)
                    logger.info('Stored data in csvArea')
                else:
                    taskOne.JSONtoCsvFacility(**x)
                    logger.info('Stored data in csvFacility')
                routingPublish = 'cd'
                i = taskOne.getCsv(filenameArea)
                j = taskOne.getCsv(filenameFacility)
                print('GetCsv was executed')
                print(i)
                print(j)
                pushRabbitMDM = PublishRabbitMQ()
                pushRabbitMDM.startImport(i, routingPublish)
                pushRabbitMDM.startImport(j, routingPublish)
                print('Csv was pushed to Queue')
                k = k + 1
                print(k)

task = Task_JsonCsv()
task.main()


        # turn one JSON into one row of a csv
        #for as long as there is JSON_data
        #while true if json_data == empty break
        #if json_data is not None:


        #if taskOne.is_empty(json_data) == False:
            #data = json.loads(json_data)
            #x = flatten(data)
            #firstkey = list(x.keys())[0]
            #taskOne = Task_JsonCsv()
            #if firstkey == 'parkingAreaOccupancy':
                #taskOne.JSONtoCsvArea(**x)
                #logger.info('Stored data in csvArea')
            #else:
                #taskOne.JSONtoCsvFacility(**x)
                #logger.info('Stored data in csvFacility')
        #else:
            # push them to the Queue
            #routingPublish = 'cd'
            #i = taskOne.getCsv(filenameArea)
            #j = taskOne.getCsv(filenameFacility)
            #print('GetCsv was executed')
            #print(i)
            #print(j)
            #pushRabbitMDM = PublishRabbitMQ()
            #pushRabbitMDM.startImport(i, routingPublish)
            #pushRabbitMDM.startImport((j, routingPublish))
            #print('Csv was pushed to Queue')




        #either csv direct
        #turn csv to dict and then dict to csv

        #routingConsume = 'cd'
        #consumeRabbitMDM = ConsumeRabbitMQ()
        #csv = consumeRabbitMDM.startConsuming(routingConsume)
        #print('Print consumed Csv')
        #print(csv)
        #taskOne.decodeCsv(csv)
        #print('Csv was decoded')


