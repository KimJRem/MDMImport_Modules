import xmltodict
import json
from traffic.RabbitMQConsumer import RabbitMQConsumer
from traffic.RabbitMQProducer import RabbitMQProducer
from traffic.mdm_logging import *

setup_logging()
logger = logging.getLogger(__name__)

class TrafficXmlToJsonNew:

    def xmlToJsonArea(self, file):
        try:
            d = xmltodict.parse(file)
            k = 0
            while k < 2:
                b = d['d2LogicalModel']['payloadPublication']['genericPublicationExtension'][
                    'parkingFacilityTableStatusPublication']['parkingAreaStatus'][k]
                b['SubscriptionID'] = 2683000
                a = json.dumps(b, indent=4)
                # print('This is one Json: ')
                # print(a)
                k = k + 1
                yield a
            logger.info('xmlToJsonArea successfully completed')
        except:
            print('Conversion to Json did not work')
            raise

    def xmlToJsonFacility(self, file):
        try:
            d = xmltodict.parse(file)
            k = 0
            while k < 10:
                b = d['d2LogicalModel']['payloadPublication']['genericPublicationExtension'][
                    'parkingFacilityTableStatusPublication']['parkingFacilityStatus'][k]
                b['SubscriptionID'] = 2683000
                a = json.dumps(b, indent=4)
                # print('This is one Json: ')
                # print(a)
                k = k + 1
                yield a
            logger.info('xmlToJsonFacility successfully completed')
        except:
            print('Conversion to JSON did not work')

            # consume from Queue
            # turn Xml to JSON
            # push them to Queue

consumer_config = json.dumps({
    "exchangeName": "topic_datas",
    "host": "localhost",
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

producer_config = json.dumps({
    "exchangeName": "topic_datas",
    "exchangeType": "direct",
    "host": "localhost",
    "routingKey": "bc"

})
producer = RabbitMQProducer(json.loads(producer_config))


def main():
    # consume from Queue
    with RabbitMQConsumer(json.loads(consumer_config)) as consumer:
        logger.info('Consume')
        consumer.consume(resolve_message)


def resolve_message(data):

    print(" [x] Receiving message %r" % data)

    traffic_data_one = TrafficXmlToJsonNew()
    traffic_data_two = TrafficXmlToJsonNew()
    json_area = traffic_data_one.xmlToJsonArea(data)
    json_facilities = traffic_data_two.xmlToJsonFacility(data)
    logger.info('Converting xml files to json has been started..')

    # push them to the Queue
    for i in json_area:
        producer.publish(i)
    for j in json_facilities:
        producer.publish(j)
    logger.info('Json files have been pushed to the queue')

main()