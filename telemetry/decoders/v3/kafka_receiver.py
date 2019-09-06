from confluent_kafka import Consumer, KafkaError
import logging
logging.basicConfig(level=logging.DEBUG) 

mylogger = logging.getLogger()

c = Consumer({
    'bootstrap.servers': 'gtat-stage-msg01',
    'group.id': 'mygroup',
    'auto.offset.reset': 'latest',
    "logger": mylogger
})

c.subscribe(['gnmi'])
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
