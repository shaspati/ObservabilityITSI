import json
from kafka import KafkaConsumer
import InperaMetrics2Splunk as im2s
import util as ut

metricsTypes = ut.metricsTypes
logType2CINameMapping = ut.logType2CINameMapping
ci_list = ut.ci_list
logFile = ut.logFile


def publishMetrics2SplunkIdx(kafka_topic, kafka_brokers):

    consumer = KafkaConsumer(
        kafka_topic,
        bootstrap_servers=kafka_brokers,
        auto_offset_reset="earliest",
        group_id="inpera_read_esp",
    )

    msg_cnt = 0
    try:
        for message in consumer:
            # print("Received message: {}".format(message.value.decode("utf-8")))
            logFile.write(message.value.decode("utf-8") + "\n")

            # print(json.loads(message.value.decode("utf-8"))["beat"]["hostname"])
            convert2Metrics(json.loads(message.value.decode("utf-8")), kafka_topic)

            msg_cnt += 1
            if msg_cnt == 200:
                break

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


def convert2Metrics(msg, kafka_topic):
    # convert the message to metrics
    # print("Converting message to metrics", msg)
    logType = msg["log_type"]
    if logType not in metricsTypes:
        print("logType not in list:", logType)
        return 0

    metric = {}
    metric["event"] = "metric"
    metric["source"] = logType
    metric["host"] = msg["beat"]["hostname"]

    metric_fields = {}
    keys = msg.keys()
    # print(keys)
    for k in keys:
        if k == "beat":
            metric_fields["inpera_topic_name"] = kafka_topic
            # print("beat", msg[k].keys())
            # metric_fields[k] = kafka_topic
            for kk in msg[k].keys():
                metric_fields[kk] = msg[k][kk]
        elif k == "message":
            # print("message", msg[k])
            # metrics[k] = msg[k]
            metric_data = getMetricObj(msg[k])
            metric_fields.update(metric_data)
        else:
            metric_fields[k] = msg[k]

    metric["fields"] = metric_fields
    # send metrics to splunk
    # check if CI name is in list
    if publish2Metrics(metric_fields["ci_name"]) == 1:
        # print pretty metrics
        # print(json.dumps(metric, indent=4))
        logFile.write(json.dumps(metric, indent=4) + "\n")
        im2s.sendMetrics2Splunk(metric)
    else:
        print("CI name not in list", metric_fields["ci_name"])
        pass

    return 1


def getMetricObj(message):
    metric_fields = {}
    for m in message.split(","):
        if len(m.split("=")) > 0:
            # trim the key and value and remove blank spaces and create key value pair
            # prefix key with metrix_name.
            metrics_name = m.split("=")[0].strip()
            metrics_value = m.split("=")[1].strip()
            # check if metrics_value is a numeric

            if metrics_name in logType2CINameMapping.values():
                metrics_key = "ci_name"
                metric_fields[metrics_key] = metrics_value

            try:
                float(metrics_value)
                metrics_key = "metric_name:" + metrics_name
                metric_fields[metrics_key] = metrics_value
            except ValueError:
                metric_fields[metrics_name] = metrics_value
    return metric_fields


def publish2Metrics(ci_name):
    if ci_name in ci_list.keys():
        return 1
    else:
        return 0
