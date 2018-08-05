import datetime
import logging
import logging.config
import pika
import json
import os.path
import pandas as pd


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


class WeatherDataTransformation:

    def __init__(self, df):
        self.df = df

    # select important columns, rename columns, convert Kelvin to Celsius, convert Date to readable format
    def weather_api_data_prep(self):
        # extract only relevant columns
        df_selected_columns = self.df[
            ['dt', 'weather_0_main', 'weather_0_description', 'main_temp', 'main_temp_min', 'main_temp_max',
             'main_pressure', 'main_humidity', 'sys_sunrise', 'sys_sunset', 'wind_speed', 'wind_deg',
             'clouds_all', 'visibility', 'rain_3h', 'snow_3h']]
        # rename columns
        df_selected_columns = df_selected_columns.rename(
            columns={"dt": "Date_unix", "weather_0_main": "General_description",
                     "weather_0_description": "Short_description",
                     "main_temp": "Temperature", "main_temp_min": "Min_Temperature",
                     "main_temp_max": "Max_Temperature", "main_pressure": "Pressure",
                     "main_humidity": "Humidity", "sys_sunrise": "Sunrise",
                     "sys_sunset": "Sunset", "wind_speed": "Wind_speed",
                     "wind_deg": "Wind_direction", "clouds_all": "Clouds",
                     "visibility": "Visibility", "rain_3h": "Rain_last3h", "snow_3h": "Snow_last3h"})
        # create three new columns with °C instead of Kelvin, alternatively get Celsius directly from API
        df_selected_columns['Temp_Celsius'] = (df_selected_columns.Temperature - 273.15)
        df_selected_columns['Min_Temperature_Celsius'] = (df_selected_columns.Min_Temperature - 273.15)
        df_selected_columns['Max_Temperature_Celsius'] = (df_selected_columns.Max_Temperature - 273.15)
        # convert date to readable format, should be the same format as the MDM data
        df_selected_columns['Date'] = df_selected_columns.apply(
            lambda row: datetime.datetime.utcfromtimestamp(row['Date_unix']).replace(tzinfo=datetime.timezone.utc),
            axis=1)
        df_selected_columns['Date'] = pd.to_datetime(df_selected_columns['Date']).dt.tz_convert('Europe/Berlin')
        return df_selected_columns


# function for min and max value for every column of the Dataframe
def all_min_max(df):
    return [df.max(), df.min()]


# function for min, max value of a specific column
def min_max(df, column_name):
    return [df[column_name].max(), df[column_name].min()]


def all_describe(df):
    print('Statistical overview of the weather data: ')
    return df.describe()


# Statistical overview of selected columns
def describe(df, column_name):
    print('Statistical overview of the column %s: ' % column_name)
    return df[column_name].describe()


def decode_csv(body):
    file_name = body.decode().split('.csv')[0]
    message = body.decode().split('.csv')[1]
    filename = '{}.csv'.format(file_name)
    with open(filename, 'w') as write_csv:
        # with open('{}.csv'.format(file_name), 'w') as write_csv:
        write_csv.write(message)
    return filename


def csv_to_df(filename):
    df = pd.read_csv(filename, error_bad_lines=False)
    return df


setup_logging()
logger = logging.getLogger(__name__)

consumer_config = json.dumps({
    "exchangeName": "topic_datas",
    "host": "127.0.0.1",
    "routingKey": "24",
    "exchangeType": "topic",
    "queueName": "12",
    "exchangeOptions": {
        "passive": True,
        "durable": True,
        "autoDelete": True,
        "internal": False
    },
    "queueOptions": {
        "passive": True,
        "durable": True,
        "exclusive": True,
        "autoDelete": True
    }
})


def main():
    while True:
        with RabbitMQConsumer(json.loads(consumer_config)) as consumer:
            consumer.consume(extract_data)


def extract_data(csv_data):
    # decode csv file
    data = decode_csv(csv_data)
    # turn csv file into DF
    df = csv_to_df(data)
    print('Print DF: ')
    logger.info(df)
    # do statistical overview
    task = WeatherDataTransformation(df)
    data_prep_df = task.weather_api_data_prep()
    logger.info(data_prep_df)

    extract_min_max(data_prep_df)
    extract_statistical_overview(data_prep_df)


def extract_min_max(data_prep):
    all_min_max_value = all_min_max(data_prep)
    logger.info(
        'The maximum values are: \n%s. \nThe minimum values are: \n%s.' % (all_min_max_value[0], all_min_max_value[1]))

    max_min_value = min_max(data_prep, 'Wind_speed')
    logger.info(
        'The maximum value of the selected column is: \n%s. \nThe minimum value of the selected column is: \n%s.' % (
            max_min_value[0], max_min_value[1]))


def extract_statistical_overview(data_prep):
    stats = all_describe(data_prep)
    logger.info(stats)
    column_stats = describe(data_prep, 'Temp_Celsius')
    print(column_stats)


if __name__ == "__main__":
    main()
