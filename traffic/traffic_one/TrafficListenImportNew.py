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

# Some code for the class MyHandler and and main() method were taken from:
# Bruno Rocha
# Source: http://brunorocha.org/python/watching-a-directory-for-file-changes-with-python.html?utm_content=bufferddcb
# 9&utm_source=buffer&utm_medium=twitter&utm_campaign=Buffer
# 12/07/2013

# Sets the logging configurations with logging.json file
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

# Class to perform some action on the event listened to by the observer
class MyHandler(PatternMatchingEventHandler):

    # only check for .xml documents
    patterns = ["*.xml"]

    # Processing of the newly created file
    def process(self, event):
        try:
            print(event.scr_path, event.event_type)  # for controlling only
            logger.info('There was one newly created file')
            # stores file input in a xml string
            with open(event.src_path, 'rb') as xml_source:
                xml_string = xml_source.read()
            # calls the producer and publishes the xml string
            producer.publish(xml_string)
            logger.info('Published Data to queue')
        except:
            logger.error('The newly created file could not be processed')
            raise

    # Only starts def process when "created" event is occurring
    def on_created(self, event):
        logger = logging.getLogger(__name__)
        try:
            logger.info('Created event occurred')
            self.process(event)
        except:
            logger.error('Created event occurred but def process could not be started ')
            raise


# Class to publish data to a RabbitMQ queue
class RabbitMQProducer:

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

    # Observer class to listen for changes
    newObserver = Observer()
    print(os.listdir("/usr/src/app")) # for controlling only
    newObserver.schedule(MyHandler(), path='/usr/src/app/output')
    #newObserver.schedule(MyHandler(), path='/Users/kim/MDMImporter/output')

    newObserver.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        newObserver.stop()

    newObserver.join()


# =========================== Main start ======================================
setup_logging()
logger = logging.getLogger(__name__)

# Configurations for producer
producer_config = json.dumps({
    "exchangeName": "topic_data",
    "exchangeType": "direct",
    "host": "rabbitmq",
    "routingKey": "ab"

})

# Initialise a new producer with its configuration
producer = RabbitMQProducer(json.loads(producer_config))

if __name__ == "__main__":
    main()
