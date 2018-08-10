import xmltodict
import json
import pika
import traceback
import os
import json
import logging
import logging.config


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


# Class to publish data to a RabbitMQ queue
class RabbitMQProducer:

    def __init__(self, config):
        # Initialize the consumer with the available configs of RabbitMQ
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

# Class to consume data from a RabbitMQ queue
class RabbitMQConsumer:

    def __init__(self, config):
        # Initialize the consumer with the available configurations of RabbitMQ
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


# Class to transform the traffic data from XML to JSON format
class TrafficXmlToJsonNew:

    # Function to transform the parkingArea XML files to several JSON files
    def xmlToJsonArea(self, file):
        try:
            d = xmltodict.parse(file)
            k = 0
            while k < 2:
                b = d['d2LogicalModel']['payloadPublication']['genericPublicationExtension'][
                    'parkingFacilityTableStatusPublication']['parkingAreaStatus'][k]
                b['SubscriptionID'] = 2683000
                a = json.dumps(b, indent=4)
                k = k + 1
                yield a
            logger.info('xmlToJsonArea successfully completed')
        except:
            logger.info('Conversion to Json did not work')
            raise

    # Function to transform the parkingFacility XML files to several JSON files
    def xmlToJsonFacility(self, file):
        try:
            d = xmltodict.parse(file)
            k = 0
            while k < 10:
                b = d['d2LogicalModel']['payloadPublication']['genericPublicationExtension'][
                    'parkingFacilityTableStatusPublication']['parkingFacilityStatus'][k]
                b['SubscriptionID'] = 2683000
                a = json.dumps(b, indent=4)
                k = k + 1
                yield a
            logger.info('xmlToJsonFacility successfully completed')
        except:
            logger.info('Conversion to JSON did not work')
            raise

# Configurations for consumer
consumer_config = json.dumps({
    "exchangeName": "topic_data",
    "host": "rabbitmq",
    "routingKey": "ab",
    "exchangeType": "direct",
    "queueName": "ab",
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

# Configurations for producer
producer_config = json.dumps({
    "exchangeName": "topic_data",
    "exchangeType": "direct",
    "host": "rabbitmq",
    "routingKey": "bc"

})
# Initialise a new producer with its configuration
producer = RabbitMQProducer(json.loads(producer_config))

setup_logging()
logger = logging.getLogger(__name__)


def main():
    # Consume from Queue
    with RabbitMQConsumer(json.loads(consumer_config)) as consumer:
        logger.info('Consume')
        consumer.consume(resolve_message)


# Resolves the message consumed from the queue
def resolve_message(data):

    print(" [x] Receiving message %r" % data)

    traffic_data_one = TrafficXmlToJsonNew()
    traffic_data_two = TrafficXmlToJsonNew()
    json_area = traffic_data_one.xmlToJsonArea(data)
    json_facilities = traffic_data_two.xmlToJsonFacility(data)
    logger.info('Converting xml files to json has been started..')

    # publishes all JSON files to the queue
    for i in json_area:
        producer.publish(i)
    for j in json_facilities:
        producer.publish(j)
    logger.info('Json files have been pushed to the queue')


if __name__ == "__main__":
    main()