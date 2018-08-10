import datetime
import os.path
import pika
import traceback
import csv
import numpy as np
import pandas as pd
import json
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

    def consume(self):
        # Consume message. It tells the broker to spin up a consumer process, which checks for messages
        # on a specified queue, and then registers a callback function. The callback function should be executed
        # when a message is available and has been delivered to the client.

        global connection
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.config['host']))
        channel = connection.channel()

        self._create_exchange(channel)
        self._create_queue(channel)

        channel.queue_bind(queue=self.config['queueName'],
                           exchange=self.config['exchangeName'],
                           routing_key=self.config['routingKey'])

        channel.basic_consume(self.callback, queue=self.config['queueName'])
        channel.start_consuming()
        return gdata

    def _create_exchange(self, channel):
        # Declare the exchange using the given communication channel
        logger.info('create exchange')
        exchange_options = self.config['exchangeOptions']
        channel.exchange_declare(exchange=self.config['exchangeName'],
                                 exchange_type=self.config['exchangeType'],
                                 passive=exchange_options['passive'],
                                 durable=exchange_options['durable'],
                                 auto_delete=exchange_options['autoDelete'],
                                 internal=exchange_options['internal'])

    def _create_queue(self, channel):
        # Create the queue to pick up the messages directly from the queue
        logger.info('create queue')
        queue_options = self.config['queueOptions']
        channel.queue_declare(queue=self.config['queueName'],
                              passive=queue_options['passive'],
                              durable=queue_options['durable'],
                              exclusive=queue_options['exclusive'],
                              auto_delete=queue_options['autoDelete'])

    def callback(self, ch, method, properties, body):
        # Callback function that will be executed
        logger.info('consume message')
        global connection
        global gdata
        gdata = body
        ch.basic_ack(delivery_tag=method.delivery_tag)
        connection.close()


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


# Class to merge weather and traffic data on timestamp
class TrafficWeatherMerge:

    # Preparation of the weather data
    # Select important columns, rename columns, convert Kelvin to Celsius, convert date
    def weather_API_dataPrep(self, df):

        df_selectedColumns = df[
            ['dt', 'weather_0_main', 'weather_0_description', 'main_temp', 'main_temp_min', 'main_temp_max',
             'main_pressure', 'main_humidity', 'sys_sunrise', 'sys_sunset', 'wind_speed', 'wind_deg',
             'clouds_all', 'visibility', 'rain_3h', 'snow_3h']]

        df_selectedColumns = df_selectedColumns.rename(
            columns={"dt": "Date_unix", "weather_0_main": "General_description",
                     "weather_0_description": "Short_description",
                     "main_temp": "Temperature", "main_temp_min": "Min_Temperature",
                     "main_temp_max": "Max_Temperature", "main_pressure": "Pressure",
                     "main_humidity": "Humidity", "sys_sunrise": "Sunrise",
                     "sys_sunset": "Sunset", "wind_speed": "Wind_speed",
                     "wind_deg": "Wind_direction", "clouds_all": "Clouds",
                     "visibility": "Visibility", "rain_3h": "Rain_last3h", "snow_3h": "Snow_last3h"})

        df_selectedColumns['Temp_Celsius'] = (df_selectedColumns.Temperature - 273.15)
        df_selectedColumns['Min_Temperature_Celsius'] = (df_selectedColumns.Min_Temperature - 273.15)
        df_selectedColumns['Max_Temperature_Celsius'] = (df_selectedColumns.Max_Temperature - 273.15)

        df_selectedColumns['Date'] = df_selectedColumns.apply(
            lambda row: datetime.datetime.utcfromtimestamp(row['Date_unix']).replace(tzinfo=datetime.timezone.utc),
            axis=1)
        df_selectedColumns['Date'] = pd.to_datetime(df_selectedColumns['Date']).dt.tz_convert('Europe/Berlin')
        df_selectedColumns.fillna(0, inplace=True)
        return df_selectedColumns

    # Preparation of the traffic data
    # Rename columns, convert date
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

    # Function to merge the two datasets on timestamp (+/- 2 minutes)
    # assuming "Short description" will be in every weather data import, drop all columns with NaN values in "Short description"
    def mergeDatasetsNew(self, dfWeather, dfTraffic):
        columnNames = list(dfTraffic.head(0))
        firstColumnName = columnNames[0]
        if firstColumnName == 'parkingAreaOccupancy':
            logger.info('mergeOne')
            newDFArea = pd.merge_asof(dfWeather, dfTraffic, on='Date', tolerance=pd.Timedelta('12000ms'))
            logger.info('mergeTwo')
            b = newDFArea.dropna(subset=['Short_description'])
            b.to_csv('areaundwetter.csv', sep='\t', encoding='utf-8')
        else:
            newDFFacility = pd.merge_asof(dfWeather, dfTraffic, on='Date', tolerance=pd.Timedelta('12000ms'))
            logger.info('mergeThree')
            c = newDFFacility.dropna(subset=['Short_description'])
            logger.info('mergeFour')
            c.to_csv('facilityundwetter.csv', sep='\t', encoding='utf-8')


# Configurations for consumer for traffic data
consumer_configOne = json.dumps({
    "exchangeName": "topic_data",
    "host": "localhost",
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

# Configurations for consumer for weather data
consumer_configTwo = json.dumps({
    "exchangeName": "topic_datas",
    "host": "localhost",
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

# Initialise the two consumers with its configuration
consumer_one = RabbitMQConsumer(json.loads(consumer_configOne))
consumer_two = RabbitMQConsumer(json.loads(consumer_configTwo))

setup_logging()
logger = logging.getLogger(__name__)


# Consume from queue, do transformations and merge data sets
def main():
    while True:
        logger = logging.getLogger(__name__)

        #consume from traffic queue
        logger.info('Consume traffic')
        y = consumer_one.consume()

        # consume from weather queue
        logger.info('Consume weather')
        x = consumer_two.consume()

        #manipulate traffic
        analysisTask = HelperClass()
        data_traffic = analysisTask.decodeCsv(y)
        dataframe_traffic = analysisTask.csvToDF(data_traffic)
        logger.info('Traffic data prepared')

        #manipulate weather
        analysisTask = HelperClass()
        data_weather = analysisTask.decodeCsv(x)
        dataframe_weather = analysisTask.csvToDF(data_weather)
        logger.info('Weather data prepared')

        #do the join
        task = TrafficWeatherMerge()
        traffic_prep = task.traffic_dataPrep(dataframe_traffic)
        weather_prep = task.weather_API_dataPrep(dataframe_weather)

        task.mergeDatasetsNew(traffic_prep, weather_prep)
        logger.info('Datasets have been merged on field data')


if __name__ == "__main__":
    main()