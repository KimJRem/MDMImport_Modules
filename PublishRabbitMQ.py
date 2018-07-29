import pika
import requests
import json
import sys
from twisted.internet import task
from twisted.internet import reactor
from logging_one import *

class PublishRabbitMQ:

    setup_logging()

    #method to look imported data in the output file. If there is new data store it in the RabbitMQ
    def startImport(self, file, routing):
        logger = logging.getLogger(__name__)
        try:
            #insert logic only if json_file not null, do this, otherwise just not send anything
            message = file
            #print(message)

            #open connection and send data to queue
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBIT_HOST))
            channel = connection.channel()
            #channel.queue_declare(queue=QUEUE_NAME)
            channel.queue_declare(queue=routing)
            #channel.exchange_declare(exchange='a', exchange_type='direct')
            #channel.basic_publish(exchange='',
                              #routing_key=QUEUE_NAME,
                              #body=message)
            channel.basic_publish(exchange='', routing_key=routing, body=message, properties=pika.BasicProperties(delivery_mode=2))
            print(" [x] Sent data to RabbitMQ")
            logger.info('The data was sent to RabbitMQ')
            connection.close()
        except:
            logger.error('Data could not be sent to RabbitMQ')
            raise

    def startImportRepeatedly(self, file, routing):
        logger = logging.getLogger(__name__)
        try:
            timeout = 1800.0 #every 1800 seconds = 30 minutes
            repeat = task.LoopingCall(self.startImport(file, routing))
            repeat.start(timeout)
            reactor.run()
            logger.info('The import started repeatedly')
        except:
            logger.error('The import could not be started repeatedly')
            raise

    #method to stop the import
    def stopImport(self):
        logger = logging.getLogger(__name__)
        try:
            #simply stops all execution of code, not sure it is an elegant solution
            logger.info('The import was stopped by exiting the system')
            sys.exit()
        except:
            logger.error('The import could not be stopped')
            raise


RABBIT_HOST = 'localhost'#find way to define that dynamically? If needed?
QUEUE_NAME = 'Rabbit_Queue'#find way to define that dynamically? If needed?
