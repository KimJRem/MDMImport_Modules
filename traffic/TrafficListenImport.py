import time
import sys
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler
from pathlib import Path
#from logging_one import *
from mdm_logging import *
from rabbitmq import RabbitMQProducer

setup_logging()
logger = logging.getLogger(__name__)

# the code was mostly taken from here:
# http://brunorocha.org/python/watching-a-directory-for-file-changes-with-python.html?utm_content=bufferddcb9&utm_source=buffer&utm_medium=twitter&utm_campaign=Buffer

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


def main():
    newObserver = Observer()
    # path of directory to look for needs to inserted in the command line
    newObserver.schedule(MyHandler(), path='/Users/kim/MDMImporter/output')
    newObserver.start()




# =========================== Main start ======================================
producer_config = json.dumps({
    "exchangeName": "topic_datas",
    "host": "localhost",
    "routingKey": "ab"

})

producer = RabbitMQProducer(json.loads(producer_config))

main()
