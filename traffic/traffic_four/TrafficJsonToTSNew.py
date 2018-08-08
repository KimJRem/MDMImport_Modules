import json
import traceback
from flatten_json import flatten
import pika
import os
import json
import logging
import logging.config


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


class TrafficJsonToTSNew:


    def transformTS(self, data_object):

        x = flatten(data_object)
        print(x)
        firstkey = list(x.keys())[0]
        if firstkey == 'parkingAreaOccupancy':
            new_dict = {'parkingAreaID':x['parkingAreaReference_@id'],
                        'parkingAreaOccupancy':x['parkingAreaOccupancy'],
                        'timestamp':x['parkingAreaStatusTime']}
            new_json = json.dumps(new_dict)
            logger.info('Json area files have been converted')
            return new_json
        else:
            new_dict = {'parkingFacilityID':x['parkingFacilityReference_@id'],
                        'parkingFacilityOccupancy':x['parkingFacilityOccupancy'],
                        'timestamp':x['parkingFacilityStatusTime']}
            new_json = json.dumps(new_dict)
            logger.info('Json facility files have been converted')
            return new_json


consumer_config = json.dumps({
    "exchangeName": "topic_datas",
    "host": "rabbitmq",
    "routingKey": "bc",
    "exchangeType": "direct",
    "queueName": "bc",
    "exchangeOptions": {
        "passive": False,
        "durable": False,
        "autoDelete": False,
        "internal": False
    },
    "queueOptions": {
        "passive": False,
        "durable": False,
        "exclusive": False,
        "autoDelete": False
    }
})

producer_config = json.dumps({
    "exchangeName": "topic_datas",
    "exchangeType": "direct",
    "host": "rabbitmq",
    "routingKey": "de"

})

producer = RabbitMQProducer(json.loads(producer_config))

setup_logging()
logger = logging.getLogger(__name__)


def main():
    # consume from Queue
    with RabbitMQConsumer(json.loads(consumer_config)) as consumer:
        logger.info('Consume')
        consumer.consume(resolve_message)


def resolve_message(data):

    print(" [x] Receiving message %r" % data)

    data_object = json.loads(data)

    task = TrafficJsonToTSNew()
    i = task.transformTS(data_object)
    logger.info('Json files have been converted to Json TS format')

    producer.publish(i)
    logger.info('Json has been pushed to queue')

if __name__ == "__main__":
    main()