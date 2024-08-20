import json
from kafka import KafkaConsumer
import InperaMetrics2Splunk as im2s

kafka_topic = "sre-sniper-logs-alln"
kafka_topic = "sre-logs-vif-alln"
kafka_broker = "173.37.82.154:9092,173.37.82.155:9092,173.37.82.156:9092,173.37.82.151:9092,173.37.82.152:9092"

consumer = KafkaConsumer(
    kafka_topic,
    bootstrap_servers=kafka_broker,
    auto_offset_reset="earliest",
    group_id="inpera_read_esp",
)


def getMessages():
    msg_cnt = 0
    try:
        for message in consumer:
            # Process the message
            print("Received message: {}".format(message.value.decode("utf-8")))
            # msg = message.value.decode('utf-8').json()
            # print(msg["beat"]["hostname"])
            # print(msg["node"]["name"])

            # print(json.loads(message.value.decode("utf-8"))["beat"]["hostname"])
            convert2Metrics(json.loads(message.value.decode("utf-8")))

            msg_cnt += 1
            if msg_cnt == 5:
                break

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


def convert2Metrics(msg):
    # convert the message to metrics
    # print("Converting message to metrics", msg)
    metric = {}
    metric["event"] = "metric"
    metric["source"] = "disk"
    metric["host"] = msg["beat"]["hostname"]
    metric_fields = {}
    print(" ")
    keys = msg.keys()
    # print(keys)
    for k in keys:
        if k == "beat":
            # print("beat", msg[k].keys())
            for kk in msg[k].keys():
                metric_fields[kk] = msg[k][kk]
        elif k == "message":
            # print("message", msg[k])
            # metrics[k] = msg[k]
            # split values bu comma and then split again by = to get key value pairs
            # print(msg[k].split(","))
            for m in msg[k].split(","):
                # print(m.split("="))
                if len(m.split("=")) > 0:
                    # trim the key and value and remove blank spaces and create key value pair
                    # prefix key with metrix_name.
                    originalkey = m.split("=")[0].strip()
                    metrics_key = "metric_name:" + m.split("=")[0].strip()
                    metrics_value = m.split("=")[1].strip()
                    # check if metrics_value is a number
                    if metrics_value.isnumeric():
                        metrics_value = int(metrics_value)
                        metric_fields[metrics_key] = metrics_value
                    else:
                        metric_fields[originalkey] = metrics_value

            # print("metric_fields", metric_fields)
        else:
            metric_fields[k] = msg[k]

    metric["fields"] = metric_fields
    # print pretty metrics
    print(json.dumps(metric, indent=4))
    # send metrics to splunk
    im2s.sendMetrics2Splunk(metric)
    return 1


getMessages()
