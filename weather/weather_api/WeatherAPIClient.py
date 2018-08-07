import threading
import time
import traceback
import os
import logging
import json
import logging.config
import pika
import requests


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


def main():
    while True:
        """ Data is pulled every 5 seconds"""
        data = api.get_data()
        body = json.dumps(data)

        producer.publish(body)
        logger.info('Published Data %s' % data)

        time.sleep(5)


# =========================== Main start ======================================
config = json.dumps({
    "exchangeName": "topic_datas",
    "host": "127.0.0.1",
    "exchangeType": "topic",
    "routingKey": "12"

})

setup_logging()
logger = logging.getLogger(__name__)

producer = RabbitMQProducer(json.loads(config))
api = WeatherAPIClient('5e6a5233ba0c66d19549e001b75053f6', 2892518)

if __name__ == "__main__":
    main()
