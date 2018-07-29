import pika
import json
import sys

import os.path
from logging_one import *


class ConsumeRabbitMQ:
    setup_logging()

    connection = None
    gdata = None

    def returnValue(self, data):
        global connection
        global gdata
        logger = logging.getLogger(__name__)
        logger.info('returnValue is working. Print consumedDataset: ')
        gdata = data
        print(gdata)
        #connection.close()

    def callback(self, ch, method, properties, body):
        logger = logging.getLogger(__name__)
        try:
            global gdata
            #data = body
            logger.info('The data was received from the queue')
            #self.returnValue(body)
            gdata = body
            print('Print gdata')
            print(gdata)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            connection.close()
            #logger.info('Should have called method returnValue()')
        except:
            logger.error('Could not receive data from queue')
            raise

    def startConsuming(self, routing):
        logger = logging.getLogger(__name__)
        #while True:
        try:
            global connection
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBIT_HOST))
            channel = connection.channel()
            #channel.queue_declare(queue=QUEUE_NAME)
            channel.queue_declare(queue=routing)
            #channel.exchange_declare(exchange='a', exchange_type='direct')
            #result = channel.queue_declare(exclusive=True)
            #queue_name = result.method.queue
            #print(queue_name)
            ##channel.queue_bind(exchange='a', queue=queue_name,routing_key=routing)
            channel.basic_consume(self.callback,
                                  queue=routing)
            #channel.basic_consume(self.callback,
                                  #queue=QUEUE_NAME,
                                  #no_ack=True)
            print(' [*] Waiting for messages. To exit press CTRL+C')
            logger.info('Waiting for messages from queue')
            channel.start_consuming()
            # while channel._consumer_infos:
            # channel.connection.process_data_events(time_limit=1)
            #connection.close()
            return gdata
        except:
            logger.error('Cannot start consuming data from queue')
            raise

RABBIT_HOST = 'localhost'  # find way to define that dynamically? If needed?
QUEUE_NAME = 'Rabbit_Queue'


