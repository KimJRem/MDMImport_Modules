#get JSON from queue and transform it into a JSON
# I get a single JSON for every Data set so it should be 1 Json -> 1 Json
from mdm_logging import *
import json
from flatten_json import flatten
from weather import WeatherAPIClient
from RabbitMQConsumer import *
from RabbitMQProducer import *


setup_logging()
logger = logging.getLogger(__name__)

class TrafficJSONToTS:


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
    "host": "localhost",
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
    "host": "localhost",
    "routingKey": "de"

})

producer = RabbitMQProducer(json.loads(producer_config))


def main():
    # consume from Queue
    with RabbitMQConsumer(json.loads(consumer_config)) as consumer:
        logger.info('Consume')
        consumer.consume(resolve_message)


def resolve_message(data):

    print(" [x] Receiving message %r" % data)

    data_object = json.loads(data)

    task = TrafficJSONToTS()
    i = task.transformTS(data_object)
    logger.info('Json files have been converted to Json TS format')

    producer.publish(i)
    logger.info('Json has been pushed to queue')

main()

