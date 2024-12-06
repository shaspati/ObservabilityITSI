from loadAlertData2GraphDB import upsertTeNode
import json
import kafka_consumer as kc

consumer = kc.getConsumer()
consumer.subscribe("teyes_events")


# modify TE alerts data
def alertData(msg):
    try:
        alert = msg["alert"]

        if "targets" in alert:
            # remove targets key because of data issues -- if needed we will reformat the data
            del alert["targets"]
        alert["name"] = alert["testName"]
        alert["event_state"] = "Active" if alert["active"] == 1 else "Cleared"
        alert["source"] = "ThousandEyes"
        print(alert)
        upsertTeNode(alert)
        return 1
    except Exception as e:
        print(f"An error occurred in alertData: {str(e)}")
        return 0


cnt = 0
alert_data = {}
for message in consumer:
    try:
        msg = json.loads(message.value)
        # print(msg)
        # update key if key name starts with @ and remove @
        alertData(msg)
        # print(msg["event_sysid"])
        cnt += 1
    except Exception as e:
        print(f"An error occurred: {str(e)}")
