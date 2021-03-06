import os.path
import csv
from csv import Dialect
import pika
import json
import unicodecsv

from flatten_json import flatten
from rabbitmq import RabbitMQProducer
from rabbitmq.RabbitMQConsumer import RabbitMQConsumer
from mdm_logging import *
from RabbitMQConsumer import *
from RabbitMQProducer import *
import sys

setup_logging()
logger = logging.getLogger(__name__)


# https://stackoverflow.com/questions/50813108/get-transferred-file-name-in-rabbitmq-using-python-pika
# for transferring csv files

class TrafficJsonToCsv:

    def JSONtoCsvArea(self, **data):
        logger = logging.getLogger(__name__)
        # filename = 'mdm_data_parkingArea.csv'
        global filenameArea
        filenameArea = 'mdm_data_parkingArea.csv'
        with open(filenameArea, "a", encoding='utf-8') as csvfile:
        #with open(filenameArea, 'rwb') as csvfile:
            fileEmpty = os.stat(filenameArea).st_size == 0
            headers = list(data)
            writer = csv.DictWriter(csvfile, delimiter=',', lineterminator='\n', fieldnames=headers)
            if fileEmpty:
                # print("Header is written now")
                logger.info('Header is written now')
                writer.writeheader()  # file doesn't exist yet, write a header
            #cannot deal with the letter "ö" in Wilhelmshöhe
            writer.writerow(data)
            # print('Received one set of data')
            logger.info('Received one set of data')
        csvfile.close()

    def JSONtoCsvFacility(self, **data):
        logger = logging.getLogger(__name__)
        # filename = 'mdm_data_parkingFacility.csv'
        global filenameFacility
        filenameFacility = 'mdm_data_parkingFacility.csv'
        #with open(filenameFacility, "ab") as csvfile:
        with open(filenameFacility, 'a', encoding='utf-8') as csvfile:
            fileEmpty = os.stat(filenameFacility).st_size == 0
            headers = list(data)
            writer = csv.DictWriter(csvfile, delimiter=',', lineterminator='\n', fieldnames=headers)
            if fileEmpty:
                # print("Header is written now")
                logger.info('Header is written now')
                writer.writeheader()
            writer.writerow(data)
            # print('Received one set of data')
            logger.info('Received one set of data')
        csvfile.close()

    def getCsv(self, filename):
        with open(filename, 'rb') as csv_file:
            return filename + csv_file.read().decode()

consumer_config = json.dumps({
    "exchangeName": "topic_datas",
    "host": "localhost",
    "routingKey": "bc",
    "exchangeType": "direct",
    "queueName": "bc",
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

producer_config = json.dumps({
    "exchangeName": "topic_datas",
    "exchangeType": "direct",
    "host": "localhost",
    "routingKey": "cd"

})

producer = RabbitMQProducer(json.loads(producer_config))

filenameArea = None
filenameFacility = None
k = 0


def main():
    # consume from Queue
    with RabbitMQConsumer(json.loads(consumer_config)) as consumer:
        logger.info('Consume')
        consumer.consume(resolve_message)


def resolve_message(data):

    print(" [x] Receiving message %r" % data)

    task = TrafficJsonToCsv()
    global k

    if k < 11:
        json_data = json.loads(data)
        #a = task.byteify(json_data)
        x = flatten(json_data)
        firstkey = list(x.keys())[0]
        # taskOne = Task_JsonCsv()
        if firstkey == 'parkingAreaOccupancy':
            print(x)
            task.JSONtoCsvArea(**x)
            logger.info('Stored data in csvArea')
        else:
            task.JSONtoCsvFacility(**x)
            logger.info('Stored data in csvFacility')
        k = k + 1
        print(k)
    else:
        json_data = json.loads(data)
        x = flatten(json_data)
        firstkey = list(x.keys())[0]
        # taskOne = Task_JsonCsv()
        if firstkey == 'parkingAreaOccupancy':
            task.JSONtoCsvArea(**x)
            logger.info('Stored data in csvArea')
        else:
            task.JSONtoCsvFacility(**x)
            logger.info('Stored data in csvFacility')

        i = task.getCsv(filenameArea)
        j = task.getCsv(filenameFacility)
        producer.publish(i)
        logger.info('Csv area files have been pushed to the queue')
        producer.publish(j)
        logger.info('Csv facility files have been pushed to the queue')
        k = k + 1
        print(k)

main()

