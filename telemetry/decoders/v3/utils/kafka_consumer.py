from confluent_kafka import Consumer, KafkaError
import sys
from optparse import OptionParser

DEFAULT_TOPIC = "daisy.test.device-avro-raw"
DEFAULT_SERVER = "kafka.sbd.corproot.net:9093"
parser = OptionParser()
parser.add_option(
    "-t",
    "--topic",,
    default=str(DEFAULT_TOPIC),
    dest="topic",
    help="Topic to listen",
)
parser.add_option(
    "-s",
    "--servers",,
    default=str(DEFAULT_SERVER),
    dest="servers",
    help="Kafka servers",
)

(options, _) = parser.parse_args()

c = Consumer({
    'bootstrap.servers': options.servers,
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest'
})

c.subscribe([options.topic])
#c.subscribe(['Cisco-IOS-XR-qos-ma-oper.qos.nodes.node.policy-map.interface-table.interface.member-interfaces.member-interface.output.service-policy-names.service-policy-instance.statistics'])

while True:
    msg = c.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    print('Received message: {}'.format(msg.value().decode('utf-8')))

c.close()
