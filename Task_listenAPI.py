import time
import sys
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler
from pathlib import Path
from PublishRabbitMQ import *
from logging_one import *


# the code was mostly taken from here:
# http://brunorocha.org/python/watching-a-directory-for-file-changes-with-python.html?utm_content=bufferddcb9&utm_source=buffer&utm_medium=twitter&utm_campaign=Buffer

class MyHandler(PatternMatchingEventHandler):
    setup_logging()
    # only check for .xml documents
    patterns = ["*.xml"]

    def process(self, event):
        logger = logging.getLogger(__name__)
        # the file will be processed there
        try:
            print(event.src_path, event.event_type)  # print now only for debug
            print("There was one newly created file")
            # logger.info(event.scr_path, event.event_type)
            logger.info('There was one newly created file')

            with open(event.src_path, 'r') as xml_source:
                xml_string = xml_source.read()
                #print(xml_string)

            routing = 'ab'
            pushRabbitMDM = PublishRabbitMQ()
            pushRabbitMDM.startImport(xml_string,routing)

        except:
            logger.error('The newly created file could not be processed')
            raise

    # only starts def process when "created" event occurred
    def on_created(self, event):
        logger = logging.getLogger(__name__)
        try:
            logger.info('Created event occured')
            self.process(event)
        except:
            logger.error('Created event occured but def process could not be started ')
            raise


# insert path to continuously check for new files in command line
if __name__ == '__main__':
    # oder den Ordner hardcoden??
    args = sys.argv[1:]
    newObserver = Observer()
    # path of directory to look for needs to inserted in the command line
    newObserver.schedule(MyHandler(), path=args[0])
    newObserver.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        newObserver.stop()

    newObserver.join()
