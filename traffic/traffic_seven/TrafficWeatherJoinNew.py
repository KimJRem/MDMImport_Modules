import datetime
import os.path
import pika
import traceback
import HelperClass
import csv
import numpy as np
import pandas as pd
import json
import os
import json
import logging
import logging.config

# https://stackoverflow.com/questions/50813108/get-transferred-file-name-in-rabbitmq-using-python-pika
# for transferring csv files

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


class TrafficWeatherJoinNew:

    # select important columns, rename columns, convert Kelvin to Celsius, convert Date to readable format
    def weather_API_dataPrep(self, df):
        # extract only relevant columns
        df_selectedColumns = df[
            ['dt', 'weather_0_main', 'weather_0_description', 'main_temp', 'main_temp_min', 'main_temp_max',
             'main_pressure', 'main_humidity', 'sys_sunrise', 'sys_sunset', 'wind_speed', 'wind_deg',
             'clouds_all', 'visibility', 'rain_3h', 'snow_3h']]
        # rename columns
        df_selectedColumns = df_selectedColumns.rename(
            columns={"dt": "Date_unix", "weather_0_main": "General_description",
                     "weather_0_description": "Short_description",
                     "main_temp": "Temperature", "main_temp_min": "Min_Temperature",
                     "main_temp_max": "Max_Temperature", "main_pressure": "Pressure",
                     "main_humidity": "Humidity", "sys_sunrise": "Sunrise",
                     "sys_sunset": "Sunset", "wind_speed": "Wind_speed",
                     "wind_deg": "Wind_direction", "clouds_all": "Clouds",
                     "visibility": "Visibility", "rain_3h": "Rain_last3h", "snow_3h": "Snow_last3h"})
        #create three new columns with °C instead of Kelvin, alternatively get Celsius directly from API
        df_selectedColumns['Temp_Celsius'] = (df_selectedColumns.Temperature - 273.15)
        df_selectedColumns['Min_Temperature_Celsius'] = (df_selectedColumns.Min_Temperature - 273.15)
        df_selectedColumns['Max_Temperature_Celsius'] = (df_selectedColumns.Max_Temperature - 273.15)
        # convert date to readable format, should be the same format as the MDM data
        df_selectedColumns['Date'] = df_selectedColumns.apply(
            lambda row: datetime.datetime.utcfromtimestamp(row['Date_unix']).replace(tzinfo=datetime.timezone.utc),
            axis=1)
        # set Date to one conform standard
        df_selectedColumns['Date'] = pd.to_datetime(df_selectedColumns['Date']).dt.tz_convert('Europe/Berlin')
        return df_selectedColumns

    # dataPrepTraffic = rename
    def traffic_dataPrep(self, dataframe):
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
            dfArea['Date'] = pd.to_datetime(dfArea['Date']).dt.tz_localize('UTC').dt.tz_convert('Europe/Berlin')
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
            dfFacility['Date'] = pd.to_datetime(dfFacility['Date']).dt.tz_localize('UTC').dt.tz_convert('Europe/Berlin')
            return dfFacility

    # merge dataset on Time (+/- 2 minutes)
    # drop all rows where column "Short_description" has NaN values, assuming "Short_description" will be in every import from the weather API
    def mergeDatasets(self, dfWeather, dfTraffic):
        newDF = pd.merge_asof(dfWeather, dfTraffic, on='Date', tolerance=pd.Timedelta('12000ms'))
        newDF.to_csv('verkehrundwetter.csv', sep='\t', encoding='utf-8')
        print(newDF)
        return newDF

    def mergeDatasetsNew(self, dfWeather, dfTraffic):
        columnNames = list(dfTraffic.head(0))
        firstColumnName = columnNames[0]
        if firstColumnName == 'parkingAreaOccupancy':
            newDFArea = pd.merge_asof(dfWeather, dfTraffic, on='Date', tolerance=pd.Timedelta('12000ms'))
            b = newDFArea.dropna(subset=['Short_description'])
            b.to_csv('areaundwetter.csv', sep='\t', encoding='utf-8')
            print(newDFArea)
        else:
            newDFFacility = pd.merge_asof(dfWeather, dfTraffic, on='Date', tolerance=pd.Timedelta('12000ms'))
            c = newDFFacility.dropna(subset=['Short_description'])
            c.to_csv('facilityundwetter.csv', sep='\t', encoding='utf-8')
            print(newDFFacility)

consumer_configOne = json.dumps({
    "exchangeName": "topic_datas",
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

consumer_configTwo = json.dumps({
    "exchangeName": "topic_datas",
    "host": "rabbitmq",
    "routingKey": "24",
    "exchangeType": "topic",
    "queueName": "24",
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

    # consume from traffic queue
    with RabbitMQConsumer(json.loads(consumer_configOne)) as consumer:
        logger.info('Consume')
        consumer.consume(resolve_messageTraffic)

    # consume from weather queue
    with RabbitMQConsumer(json.loads(consumer_configTwo)) as consumer:
        logger.info('Consume')
        consumer.consume(resolve_messageWeather)


def resolve_messageWeather(data):

    task = TrafficWeatherJoinNew()
    print(" [x] Receiving message %r")
    analysisTask = HelperClass()
    dataWeather = analysisTask.decodeCsv(data)
    dfWeather = analysisTask.csvToDF(dataWeather)
    weatherPrep_DF = task.weather_API_dataPrep(dfWeather)
    print('Print DF: ')
    print(weatherPrep_DF)
    return weatherPrep_DF


def resolve_messageTraffic(data):

    task = TrafficWeatherJoinNew()
    print(" [x] Receiving message %r")
    analysisTask = HelperClass()
    dataTraffic = analysisTask.decodeCsv(data)
    dfTraffic = analysisTask.csvToDF(dataTraffic)
    trafficPrep_DF = task.traffic_dataPrep(dfTraffic)
    print('Print DF: ')
    print(trafficPrep_DF)
    return trafficPrep_DF

# do not how to call this method
def combineInputs(dfTraffic, dfWeather):

    task = TrafficWeatherJoinNew()
    task.mergeDatasetsNew(dfTraffic, dfWeather)

main()