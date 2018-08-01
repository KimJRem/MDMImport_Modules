# class to transform the data from both APIs into the correct format for the TSR
import csv
import os.path
from flatten_json import flatten
from logging_one import *
from ConsumeRabbitMQ import *
from PublishRabbitMQ import *


class Task_WeatherJsonToCsv:
    setup_logging()

    # get data from RabbitMQ and transform to the format needed by the TSR
    # or use this functions within the ReceivedRabbitMQ class?

    filename = ''

    def JSONtoCsvComplete(self, **x):
        logger = logging.getLogger(__name__)
        # opens a csv file and stores the entries of the RabbitMQ in the csv file
        try:
            global filename
            filename = 'weather_data_all.csv'
            with open(filename, "a") as csvfile:
                fileEmpty = os.stat(filename).st_size == 0
                # main.sea_level, main.grnd_level not measured for Kassel
                headers = ['coord_lon', 'coord_lat', 'weather_0_id', 'weather_0_main', 'weather_0_description',
                           'weather_0_icon', 'base',
                           'main_temp', 'main_pressure', 'main_humidity', 'main_temp_min', 'main_temp_max',
                           'visibility',
                           'wind_speed', 'wind_deg', 'clouds_all', 'rain_3h', 'snow_3h', 'dt', 'sys_type', 'sys_id',
                           'sys_message',
                           'sys_country', 'sys_sunrise', 'sys_sunset', 'id', 'name', 'cod']
                writer = csv.DictWriter(csvfile, delimiter=',', lineterminator='\n', fieldnames=headers)
                if fileEmpty:
                    print("Header is written now")
                    logger.info('Header is written now')
                    writer.writeheader()  # file doesn't exist yet, write a header
                writer.writerow(x)
                print('Received one set of data')
                logger.info('Received one set of data')
        except:
            logger.error('Could not create and write to csv file')
            raise

    def getCsv(self, filename):
        with open(filename, 'rb') as csv_file:
            return filename + csv_file.read().decode()


def main():
    while True:
        logger = logging.getLogger(__name__)
        # consume from Queue
        consumeRabbitMDM = ConsumeRabbitMQ()
        logger.info('First')
        routingConsume = '12'
        json_data = consumeRabbitMDM.startConsuming(routingConsume)
        logger.info('Second')
        print(json_data)
        logger.info('Third')

        # turn one XML to several JSON
        task = Task_WeatherJsonToCsv()

        data = json.loads(json_data)  # transforms Json to python object
        # then do some transformation on the Python object
        # and store it back into a JSON type, maybe do the transformation in own class
        # print(json.dumps(data))
        x = flatten(data)
        task.JSONtoCsvComplete(**x)
        logger.info('Four')

        # push them to the Queue
        i = task.getCsv(filename)

        routingPublish = '24'
        pushRabbitMDM = PublishRabbitMQ()
        pushRabbitMDM.startImport(i, routingPublish)
        logger.info('Csv was pushed to queue')


main()
