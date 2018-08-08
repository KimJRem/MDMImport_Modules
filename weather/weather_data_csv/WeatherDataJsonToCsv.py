import csv
import json
import logging
import logging.config
import os.path
import traceback

import pika
from flatten_json import flatten


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
                                     passive=False)
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


class RabbitMQConsumer:
    """RabbitMQ Consumer Implementation in Python"""

    def __init__(self, config):
        # Initialize the consumer with the available configs of rabbitMQ
        self.config = config

    def __enter__(self):
        # Open the connection
        self.connection = self._create_connection()
        return self

    def __exit__(self, *args):
        # Close the connection
        self.connection.close()

    def consume(self, message_received_callback):
        # Consume message. It tells the broker to spin up a consumer process, which checks for messages
        # on a specified queue, and then registers a callback function. The callback function should be executed
        # when a message is available and has been delivered to the client.
        self.message_received_callback = message_received_callback

        channel = self.connection.channel()

        self._create_exchange(channel)
        self._create_queue(channel)

        channel.queue_bind(queue=self.config['queueName'],
                           exchange=self.config['exchangeName'],
                           routing_key=self.config['routingKey'])

        channel.basic_consume(self._consume_message, queue=self.config['queueName'])
        channel.start_consuming()

    def _create_exchange(self, channel):
        # Declare the exchange using the given communication channel
        exchange_options = self.config['exchangeOptions']
        channel.exchange_declare(exchange=self.config['exchangeName'],
                                 exchange_type=self.config['exchangeType'],
                                 passive=exchange_options['passive'],
                                 durable=exchange_options['durable'],
                                 auto_delete=exchange_options['autoDelete'],
                                 internal=exchange_options['internal'])

    def _create_queue(self, channel):
        # Create the queue to pick up the messages directly from the queue
        queue_options = self.config['queueOptions']
        channel.queue_declare(queue=self.config['queueName'],
                              passive=queue_options['passive'],
                              durable=queue_options['durable'],
                              exclusive=queue_options['exclusive'],
                              auto_delete=queue_options['autoDelete'])

    def _create_connection(self):
        # Open a new connection if it is not existing
        parameters = pika.ConnectionParameters(host=self.config['host'])
        return pika.BlockingConnection(parameters)

    def _consume_message(self, channel, method, properties, body):
        # Callback function that will be executed
        self.message_received_callback(body)
        channel.basic_ack(delivery_tag=method.delivery_tag)


class WeatherDataJsonToCsv:

    def __init__(self, filename):
        self.filename = filename

    def convert_json_to_csv(self, **x):
        # opens a csv file and stores the entries of the RabbitMQ in the csv file
        try:
            with open(self.filename, "a") as csv_file:
                file_empty = os.stat(self.filename).st_size == 0
                # main.sea_level, main.grnd_level not measured for Kassel
                headers = ['coord_lon', 'coord_lat', 'weather_0_id', 'weather_0_main', 'weather_0_description',
                           'weather_0_icon', 'base',
                           'main_temp', 'main_pressure', 'main_humidity', 'main_temp_min', 'main_temp_max',
                           'visibility',
                           'wind_speed', 'wind_deg', 'clouds_all', 'rain_3h', 'snow_3h', 'dt', 'sys_type', 'sys_id',
                           'sys_message',
                           'sys_country', 'sys_sunrise', 'sys_sunset', 'id', 'name', 'cod']
                writer = csv.DictWriter(csv_file, delimiter=',', lineterminator='\n', fieldnames=headers)
                if file_empty:
                    logger.info('Header is written now')
                    writer.writeheader()  # file doesn't exist yet, write a header
                writer.writerow(x)
                print('Received one set of data')
                logger.info('Received one set of data')
        except Exception as e:
            print(repr(e))
            traceback.print_exc()
            raise

    def get_csv(self):
        with open(self.filename, 'rb') as csv_file:
            return self.filename + csv_file.read().decode()


# =========================== Main start ======================================


consumer_config = json.dumps({
    "exchangeName": "topic_datas",
    "host": "rabbitmq",
    "routingKey": "12",
    "exchangeType": "topic",
    "queueName": "12",
    "exchangeOptions": {
        "passive": False,
        "durable": False,
        "autoDelete": False,
        "internal": False
    },
    "queueOptions": {
        "passive": False,
        "durable": False,
        "exclusive": True,
        "autoDelete": False
    }
})

producer_config = json.dumps({
    "exchangeName": "topic_datas",
    "exchangeType": "topic",
    "host": "rabbitmq",
    "routingKey": "24"

})
producer = RabbitMQProducer(json.loads(producer_config))

setup_logging()
logger = logging.getLogger(__name__)


def main():
    while True:
        # consume from Queue
        with RabbitMQConsumer(json.loads(consumer_config)) as consumer:
            logger.info('Consume')
            consumer.consume(resolve_message)


def resolve_message(data):
    print(" [x] Receiving message %r" % data)

    weather_json_csv = WeatherDataJsonToCsv('weather_data_all.csv')

    json_data = json.loads(data)  # transforms Json to python object
    # then do some transformation on the Python object
    # and store it back into a JSON type, maybe do the transformation in own class
    # print(json.dumps(data))
    flatten_data = flatten(json_data)
    weather_json_csv.convert_json_to_csv(**flatten_data)
    logger.info('Converting json to csv has been started ...')

    # push them to the Queue
    csv_data = weather_json_csv.get_csv()

    producer.publish(csv_data)
    logger.info('CSV file has been pushed to the queue')


if __name__ == "__main__":
    main()
