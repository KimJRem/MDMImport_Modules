import threading
import traceback

import requests

from mdm_logging import *
from rabbitmq import RabbitMQProducer

setup_logging()
logger = logging.getLogger(__name__)


class WeatherAPIClient:

    def __init__(self, app_id, city_id):
        self.url = "http://api.openweathermap.org/data/2.5/weather" + '?appid=' + app_id + '&id=' + str(city_id)
        logger.info('URL was build ' + self.url)

    def get_data(self):
        try:
            json_data = requests.get(self.url).json()
            logger.info('JSON data was pulled from API')
            return json_data
        except Exception as e:
            print(repr(e))
            traceback.print_exc()
            raise


def set_interval(interval):
    def decorator(fnc):
        def wrapper(*args, **kwargs):
            stopped = threading.Event()

            def loop():  # executed in another thread
                while not stopped.wait(interval):  # until stopped
                    fnc(*args, **kwargs)

            t = threading.Thread(target=loop)
            t.daemon = True  # stop if the program exits
            t.start()
            return stopped

        return wrapper

    return decorator


@set_interval(5)
def main():
    """ Data is pulled every 5 seconds"""
    data = api.get_data()
    body = json.dumps(data)

    producer.publish(body)
    logger.info('Published Data %s' % data)


# =========================== Main start ======================================
config = json.dumps({
    "exchangeName": "topic_datas",
    "host": "localhost",
    "routingKey": "12"

})

producer = RabbitMQProducer(json.loads(config))
api = WeatherAPIClient('5e6a5233ba0c66d19549e001b75053f6', 2892518)

main()
