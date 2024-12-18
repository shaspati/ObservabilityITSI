# Importing all the required package to run the consumer code, please make sure that all of the below packages are available before you run the code: json, confluent_kafka, requests

import json
from json import loads
from confluent_kafka import Consumer, KafkaException, KafkaError

from confluent_kafka import TopicPartition as TP
import deltaLoadESPData as dl

from espUtil import (
    bootstrap_server,
    consumer_group_Id,
    session_timeout_ms,
    auto_offset_reset,
    enable_auto_offset_store,
    sasl_username,
    app_id,
    obj_name,
    secret_timeout,
    compression_type,
    security_protocol,
    sasl_mechanisms,
    client_cert,
    client_key,
    topic_to_consume,
)

import requests

# Function to build the CyberArk URL which will be used in getCyberArkPwd() to retrieve the password from CyberArk SAFE
# Note: Please do not modify the below function.


def buildCorePASURL():
    corePAS_url = "https://ccp.cisco.com/AIMWebService/api/Accounts"
    complete_url = ""
    appId = app_id
    objName = obj_name

    if app_id is not None and obj_name is not None:
        complete_url = (
            "https://ccp.cisco.com/AIMWebService/api/Accounts?AppID="
            + str(appId)
            + "&Object="
            + str(objName)
        )
    else:
        print("Environment variables for cyberark not set")

    return complete_url


# Function to retrieve the password from CyberArk SAFE with the parameters mentioned in the utils.py file
# Note: Please do not modify the below function.


def getCyberArkPwd():
    url = buildCorePASURL()
    corePAS_headers = {"Content-Type": "application/json"}
    if url != "":
        try:
            response = requests.get(
                url,
                headers=corePAS_headers,
                timeout=secret_timeout,
                cert=(client_cert, client_key),
            )
        except requests.Timeout as e:
            print("CorePAS request timeout {}", e)
            return None
        except requests.ConnectionError as e:
            print("CorePAS connection error {}", e)
            return None
        except requests.RequestException as e:
            print("CorePAS request error {}", e)
            return None

        if response.status_code not in [200, 201]:
            print(
                "Status: {}",
                response.status_code,
                "Headers: {}",
                response.headers,
                "Error Response: {}",
                response.json(),
            )
            return None
        else:
            try:
                data = response.json()
                cyber_pwd = data["Content"]
                return cyber_pwd
            except requests.JSONDecodeError as e:
                print("CorePAS response parse error {}", e)
                return None


class KConsumer:
    # confcon = None
    # sasl_password = None
    def __init__(self, group):
        self.sasl_password = (
            getCyberArkPwd()
        )  # Retrieving the password from the CyberArk Safe
        if group != None:
            group_id = group

        else:
            print("Group_Id Should not be None")
        # print(bootstrap_server,consumer_group_Id,session_timeout_ms,auto_offset_reset,enable_auto_offset_store,sasl_username,app_id,obj_name,secret_timeout,compression_type,security_protocol,sasl_mechanisms)
        configs = {
            "bootstrap.servers": bootstrap_server,
            "sasl.username": sasl_username,
            "sasl.password": self.sasl_password,
            "security.protocol": security_protocol,
            "sasl.mechanisms": sasl_mechanisms,
            "group.id": group_id,
            "session.timeout.ms": session_timeout_ms,
            "auto.offset.reset": auto_offset_reset,
            "enable.auto.offset.store": enable_auto_offset_store,
            "compression.type": compression_type,
        }
        self.confcon = Consumer(configs)


Kcon = KConsumer(consumer_group_Id)


# ************************* METHOD 1 *************************
# Passing Topic only
# Kcon.confcon.subscribe([topic_to_consume]) # Here topic_to_consume is the topic name which you need to set in util.py

# ************************* METHOD 2 *************************
# Passing Topic and the partition
# parti = TP(topic_to_consume,2) # Here topic_to_consume is the topic name and 2 is the partition number
# Kcon.confcon.assign([parti])
# Note: Please reach out to the dev team for the partition number.


# ************************* METHOD 3 *************************
# Passing Topic, partition, and the offset
# parti = TP(topic_to_consume,2,1784055) # Here topic_to_consume is the topic name, 2 is the partition number, 1784055 is the offset number
# Kcon.confcon.assign([parti])
# Kcon.confcon.seek(parti)
# Note: Please reach out to the dev team for the partition number.

parti = TP(
    topic_to_consume, 2
)  # Here topic_to_consume is the topic name and 2 is the partition number
Kcon.confcon.assign([parti])
cnt = 0
try:
    print("Listening....")
    while True:
        msg = Kcon.confcon.poll(timeout=1.0)
        # end_offsets = Kcon.end_offsets([parti])
        if msg is None:
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event
                print(
                    "{} {} reached end at offset {}",
                    msg.topic(),
                    msg.partition(),
                    msg.offset(),
                )
            elif msg.error():
                raise KafkaException(msg.error())
        else:
            if msg.value() and msg.key():
                class_name = msg.key().decode()

                if class_name == "cmdb_ci_appl":
                    ci_data = json.loads(msg.value())
                    # print(ci_data)
                    dl.convert_kafka_msg_to_graph_obj(ci_data, class_name)


finally:
    # Close down consumer to commit final offsets.
    Kcon.confcon.close()
