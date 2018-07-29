import xmltodict
import json
from logging_one import *
from ConsumeRabbitMQ import *
from PublishRabbitMQ import *

class Task_XMLJSON:

    setup_logging()

    def xmlToJsonArea(self, file):
        logger = logging.getLogger(__name__)
        try:
                d = xmltodict.parse(file)
                k = 0
                while k < 2:
                    b = d['d2LogicalModel']['payloadPublication']['genericPublicationExtension'][
                        'parkingFacilityTableStatusPublication']['parkingAreaStatus'][k]
                    b['SubscriptionID'] = 2683000
                    a = json.dumps(b, indent=4)
                    #print('This is one Json: ')
                    #print(a)
                    k = k + 1
                    yield a
                logger.info('xmlToJsonArea successfully completed')
        except:
            print('Conversion to Json did not work')
            raise

    def xmlToJsonFacility(self, file):
        logger = logging.getLogger(__name__)
        try:
                d = xmltodict.parse(file)
                k = 0
                while k < 10:
                    b = d['d2LogicalModel']['payloadPublication']['genericPublicationExtension'][
                        'parkingFacilityTableStatusPublication']['parkingFacilityStatus'][k]
                    b['SubscriptionID'] = 2683000
                    a = json.dumps(b, indent=4)
                    #print('This is one Json: ')
                    #print(a)
                    k = k + 1
                    yield a
                logger.info('xmlToJsonFacility successfully completed')
        except:
            print('Conversion to JSON did not work')

            # consume from Queue
            # turn Xml to JSON
            # push them to Queue

def main():
    while True:
        logger = logging.getLogger(__name__)
        # consume from Queue
        consumeRabbitMDM = ConsumeRabbitMQ()
        logger.info('First')
        routingConsume = 'ab'
        data = consumeRabbitMDM.startConsuming(routingConsume)
        logger.info('Second')
        print(data)
        logger.info('Third')

        # turn one XML to several JSON
        taskOne = Task_XMLJSON()
        taskTwo = Task_XMLJSON()
        json_files_area = taskOne.xmlToJsonArea(data)
        logger.info('Four')
        json_files_facilities = taskTwo.xmlToJsonFacility(data)
        logger.info('Five')

        # push them to the Queue
        routingPublish = 'bc'
        pushRabbitMDM = PublishRabbitMQ()
        for i in json_files_area:
            pushRabbitMDM.startImport(i, routingPublish)
        for j in json_files_facilities:
            pushRabbitMDM.startImport(j, routingPublish)

main()