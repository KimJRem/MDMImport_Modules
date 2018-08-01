import requests
from logging_one import *
# temperature in Kelvin
# pressure in hPa
# humidity in %
# wind speed in meter/sec
# wind direction in degrees (of the wind dire)
# cloud in %
# sunrise/sunset in unix, UTC
# visibility in meter
import json
import time
from PublishRabbitMQ import *
from twisted.internet import task
from twisted.internet import reactor


class Task_WeatherRequestAPI:
    setup_logging()

    URL = ''

    def buildURL(self, appID, cityID):
        global URL
        URL = "http://api.openweathermap.org/data/2.5/weather"
        logger = logging.getLogger(__name__)
        try:
            complete_url = URL + '?appid=' + appID + '&id=' + str(cityID)
            logger.info('URL was build ' + complete_url)
            return complete_url
        except:
            logger.error('Could not build URL')
            raise

    def getDataAsJSON(self, complete_url):
        logger = logging.getLogger(__name__)
        try:
            json_data = requests.get(complete_url).json()
            logger.info('JSON data was pulled from API')
            return json_data
        except:
            logger.error('Could not pull JSON data from openweathermap API')
            raise


def main():
    while True:
        logger = logging.getLogger(__name__)
        task = Task_WeatherRequestAPI()
        url = task.buildURL('5e6a5233ba0c66d19549e001b75053f6', 2892518)
        logger.info('URL was successfully build')
        json_data = task.getDataAsJSON(url)
        print(json)
        body = json.dumps(json_data)
        logger.info('Data gotten as Json')

        # push them to the Queue
        routingPublish = '12'
        pushRabbitMDM = PublishRabbitMQ()
        pushRabbitMDM.startImport(body, routingPublish)
        logger.info('Data was pushed to queue')

        # puts the while loop to sleep for half an hour
        # weather data is updated approx. every 30 minutes
        time.sleep(1800)


main()
