from loadAlertData2GraphDB import upsertZabbixNode
import json
import kafka_consumer as kc

consumer = kc.getConsumer()
consumer.subscribe("Zabbix_Events")


# modify alerts data
def alertData(data):
    upd_keys = []
    for key in data.keys():
        if key.startswith("@"):
            upd_keys.append(key)

    for key in upd_keys:
        del data[key]

    data["name"] = data["event_entity_name"]
    data["source"] = "Zabbix"
    upsertZabbixNode(data)
    return data


cnt = 0
for message in consumer:
    try:
        msg = json.loads(message.value)
        # print(msg)
        # update key if key name starts with @ and remove @
        # print(msg["event_sysid"])
        data = alertData(msg)
        cnt += 1
    except Exception as e:
        print(f"An error occurred: {str(e)}")
