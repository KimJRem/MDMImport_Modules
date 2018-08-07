import time
import sys
import os
import logging
import logging.config
import json
import pika
import traceback
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler

# the code was mostly taken from here:
# http://brunorocha.org/python/watching-a-directory-for-file-changes-with-python.html?utm_content=bufferddcb9&utm_source=buffer&utm_medium=twitter&utm_campaign=Buffer

def setup_logging(
        default_path='./logging.json',
        default_level=logging.INFO):
    """Setup logging configuration

    """
    path = default_path
    if os.path.exists(path):
        with open(path, 'rt') as f:
            config = json.load(f)
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=default_level)

class MyHandler(PatternMatchingEventHandler):

    # only check for .xml documents
    patterns = ["*.xml"]

    def process(self, event):
        # the file will be processed there
        try:
            print(event.src_path, event.event_type)  # print now only for debug
            print("There was one newly created file")
            # logger.info(event.scr_path, event.event_type)
            logger.info('There was one newly created file')

            with open(event.src_path, 'rb') as xml_source:
                xml_string = xml_source.read()

            producer.publish(xml_string)
            logger.info('Published Data to queue')


        except:
            logger.error('The newly created file could not be processed')
            raise

    # only starts def process when "created" event occurred
    def on_created(self, event):
        logger = logging.getLogger(__name__)
        try:
            logger.info('Created event occurred')
            self.process(event)
        except:
            logger.error('Created event occurred but def process could not be started ')
            raise

class RabbitMQProducer:
    """ RabbitMQ Producer Implementation in Python"""

    def __init__(self, config):
        # Initialize the consumer with the available configs of rabbitMQ
        self.config = config

    def publish(self, message):
        # Publish message to an exchange by setting up a communication channel
        connection = None
        try:
            connection = self._create_connection()
            channel = connection.channel()

            channel.exchange_declare(exchange=self.config['exchangeName'],
                                     exchange_type=self.config['exchangeType'],
                                     passive=True)
            channel.basic_publish(exchange=self.config['exchangeName'],
                                  routing_key=self.config['routingKey'],
                                  body=message)

            print(" [x] Sent message %r" % message)
        except Exception as e:
            print(repr(e))
            traceback.print_exc()
            raise e
        finally:
            if connection:
                connection.close()

    def _create_connection(self):
        # Establish a connection with the RabbitMQ server
        parameters = pika.ConnectionParameters(host=self.config['host'])
        return pika.BlockingConnection(parameters)

def main():
    newObserver = Observer()
    # path of directory to look for needs to inserted in the command line
    newObserver.schedule(MyHandler(), path='/Users/kim/MDMImporter/output')
    newObserver.start()




# =========================== Main start ======================================
setup_logging()
logger = logging.getLogger(__name__)

producer_config = json.dumps({
    "exchangeName": "topic_datas",
    "exchangeType": "direct",
    "host": "rabbitmq",
    "routingKey": "ab"

})

producer = RabbitMQProducer(json.loads(producer_config))

main()
