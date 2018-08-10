import csv
import numpy as np
import pandas as pd
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
        # Initialize the consumer with the available configs of RabbitMQ
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

# Class with helping functions
class HelperClass:

    # Decode a csv file and return csv filename
    def decodeCsv(self, body):
        file_name = body.decode().split('.csv')[0]
        message = body.decode().split('.csv')[1]
        filename = '{}.csv'.format(file_name)
        with open(filename, 'w', encoding='utf-8') as write_csv:
            write_csv.write(message)
        return filename

    # Transform csv into a pandas Dataframe
    def csvToDF(self, filename):
        df = pd.read_csv(filename, error_bad_lines=False)
        return df


# Analysis class for the traffic data
class TrafficFilterLargestNew:

    # Rename column names to more readable column names
    def renameColumns(self, dataframe):
        columnNames = list(dataframe.head(0))
        firstColumnName = columnNames[0]

        if firstColumnName == 'parkingAreaOccupancy':
            dfArea = dataframe.rename(
                columns={"parkingAreaOccupancy": "OccupancyRate", "parkingAreaReference_@targetClass": "TargetClass",
                         "parkingAreaReference_@id": "ParkingAreaID", "parkingAreaReference_@version": "Version",
                         "parkingAreaStatusTime": "Date",
                         "parkingAreaTotalNumberOfVacantParkingSpaces": "TotalNumberOfVacantParkingSpaces",
                         "totalParkingCapacityLongTermOverride": "ParkingCapacityLongTerm",
                         "totalParkingCapacityShortTermOverride": "ParkingCapacityShortTerm"})
            return dfArea
        else:
            dfFacility = dataframe.rename(
                columns={"parkingFacilityOccupancy": "OccupancyRateFacility",
                         "parkingFacilityReference_@targetClass": "TargetClass",
                         "parkingFacilityReference_@id": "ParkingFacilityID",
                         "parkingFacilityReference_@version": "Version",
                         "parkingFacilityStatus": "Status", "parkingFacilityStatusTime": "Date",
                         "totalNumberOfOccupiedParkingSpaces": "TotalNumberOfOccupiedParkingSpaces",
                         "totalNumberOfVacantParkingSpaces": "TotalNumberOfVacantParkingSpaces",
                         "totalParkingCapacityOverride": "ParkingCapacity",
                         "totalParkingCapacityShortTermOverride": "ParkingCapacityShortTerm"})
            return dfFacility

    # Check for most popular parking space by occupancy rate
    def popularParkingbyID(self, df):
        columnNames = list(df.head(0))
        firstColumnName = columnNames[0]
        # ensures function works with and without renamed columns
        if firstColumnName == 'parkingAreaOccupancy':
            df_filter = df[firstColumnName].groupby(df['parkingAreaReference_@id']).mean()
            i = df_filter.nlargest(4)
        elif firstColumnName == 'OccupancyRate':
            df_filter = df[firstColumnName].groupby(df['ParkingAreaID']).mean()
            i = df_filter.nlargest(4)
        elif firstColumnName == 'parkingFacilityOccupancy':
            # take only those parkingFacilities with "open" status into consideration
            df = df[df.parkingFacilityStatus == 'open']
            df_filter = df[firstColumnName].groupby(df['parkingFacilityReference_@id']).mean()
            i = df_filter.nlargest(4)
        elif firstColumnName == 'OccupancyRateFacility':
            df = df[df.Status == 'open']
            df_filter = df[firstColumnName].groupby(df['ParkingFacilityID']).mean()
            i = df_filter.nlargest(4)
        return i


# Configurations for consumer
consumer_config = json.dumps({
    "exchangeName": "topic_data",
    "host": "rabbitmq",
    "routingKey": "cd",
    "exchangeType": "direct",
    "queueName": "cd",
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

    analysisTask = HelperClass()

    # decode csv file
    decoded_csv = analysisTask.decodeCsv(data)
    # turn csv file into DF
    df = analysisTask.csvToDF(decoded_csv)
    logger.info('DF has been created')

    # do statistical overview
    task = TrafficFilterLargestNew()
    renamed_DF = task.renameColumns(df)
    filter = task.popularParkingbyID(renamed_DF)
    print('Print Filter: ')
    print(filter)
    logger.info('Filter has been applied to the data set')

if __name__ == "__main__":
    main()