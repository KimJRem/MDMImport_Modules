import json
import logging
import logging.config
import os.path
import requests

import pika


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


class WeatherTimeseries:

    def __init__(self, user, passwd):
        keycloak_url = "https://hahnpro.com/auth/realms/hpc/protocol/openid-connect/token"
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        # '05f4b57d-5cfd-4981-a14f-a3c0db2c8c70'
        auth = (user, passwd)
        data = {"grant_type": "client_credentials"}
        res = requests.post(keycloak_url, auth=auth, headers=headers, data=data)
        token = res.json()["access_token"]
        logger.log("token: " + token[:5] + "..." + str(len(token)))
        self.headers = {'Authorization': 'Bearer ' + token,
                   'Content-Type': 'application/json'}

    def sendData(self, data, ts_id):
        """ This method save the temperature"""

        timestamp = data["dt"]
        temp = data["main"]["temp"]

        data_prep = {timestamp: temp}

        requests.post("https://staging.hahnpro.com/api/tsm/" + ts_id, headers=self.headers, data=json.dumps(data_prep))



# =========================== Main start ======================================

consumer_config = json.dumps({
    "exchangeName": "topic_datas",
    "host": "rabbitmq",
    "routingKey": "ts",
    "exchangeType": "topic",
    "queueName": "ts",
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


setup_logging()
logger = logging.getLogger(__name__)
weather_timeseries = WeatherTimeseries("kim-remmers@web.de", "user1234")
ts_id = ""


def main():
    while True:
        # consume from Queue
        with RabbitMQConsumer(json.loads(consumer_config)) as consumer:
            logger.info('Consume')
            consumer.consume(resolve_message)


def resolve_message(data):
    print(" [x] Sending to timeseries %r" % data)

    weather_timeseries.sendData(data, ts_id)


if __name__ == "__main__":
    main()
