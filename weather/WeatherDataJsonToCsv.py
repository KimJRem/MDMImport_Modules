import csv
import json
import logging
import os.path
import traceback

from flatten_json import flatten

from mdm_logging import setup_logging
from rabbitmq import RabbitMQProducer
from rabbitmq.RabbitMQConsumer import RabbitMQConsumer

setup_logging()
logger = logging.getLogger(__name__)


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
                    print("Header is written now")
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


consumer_config = json.dumps({
    "exchangeName": "topic_datas",
    "host": "127.0.0.1",
    "routingKey": "12",
    "exchangeType": "direct",
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
    "host": "127.0.0.1",
    "routingKey": "24"

})
producer = RabbitMQProducer(json.loads(producer_config))


def main():
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


main()
