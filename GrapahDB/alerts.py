from kafka import KafkaConsumer
from loadData2GrapDB import upsertNode
import json
import util as utl

consumer = KafkaConsumer(
    bootstrap_servers=utl.bootstrap_servers,
    group_id=utl.group_id,
    security_protocol=utl.security_protocol,
    sasl_mechanism=utl.sasl_mechanism,
    sasl_plain_username=utl.sasl_plain_username,
    sasl_plain_password=utl.sasl_plain_password,
)
# only zabbix alerts to test for now
consumer.subscribe(utl.topic_name)

cnt = 0
for message in consumer:
    # print(message.value)
    # process the message
    msg = json.loads(message.value)
    # process the alert
    try:
        # print(msg)
        # update key if key name starts with @ and remove @
        upsertNode(msg)
        cnt += 1
    except Exception as e:
        print(f"An error occurred: {str(e)}")
