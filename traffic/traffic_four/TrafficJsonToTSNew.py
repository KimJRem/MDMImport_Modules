import json
from flatten_json import flatten
from traffic.RabbitMQConsumer import RabbitMQConsumer
from traffic.RabbitMQProducer import RabbitMQProducer
from traffic.mdm_logging import *

setup_logging()
logger = logging.getLogger(__name__)


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

    task = TrafficJsonToTSNew()
    i = task.transformTS(data_object)
    logger.info('Json files have been converted to Json TS format')

    producer.publish(i)
    logger.info('Json has been pushed to queue')

main()