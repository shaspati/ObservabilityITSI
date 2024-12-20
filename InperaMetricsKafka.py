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
        auto_offset_reset="latest",
        group_id="inpera_read_esp",
    )

    msg_cnt = 0
    try:
        for message in consumer:
            # print("Received message: {}".format(message.value.decode("utf-8")))
            try:

                metric_rec = message.value.decode("utf-8")
                # logFile.write(metric_rec + "\n")

                # print(json.loads(metric_rec)
                convert2Metrics(json.loads(metric_rec), kafka_topic)

                msg_cnt += 1
                # if msg_cnt == 5:
                #    break
            except json.JSONDecodeError as e:
                print(f"JSON decode error: {e}")
                logFile.write(f"JSON decode error: {e}\n")
            except Exception as e:
                print("Error in publishMetrics2SplunkIdx", e)
                logFile.write("Error in publishMetrics2SplunkIdx" + str(e) + message)

    except KeyboardInterrupt:
        print("Process interrupted by user")
    except Exception as e:
        print(f"Error in Kafka consumer: {e}")
        logFile.write(f"Error in Kafka consumer: {e}\n")
    finally:
        consumer.close()


def convert2Metrics(msg, kafka_topic):
    try:
        # convert the message to metrics
        logType = msg["log_type"]
        if logType not in metricsTypes:
            # print("logType not in list:", logType)
            return 0

        metric = {
            "event": "metric",
            "source": logType,
        }

        metric_fields = {}
        keys = msg.keys()
        # print(keys)
        for k in keys:
            try:
                if k == "@metadata":
                    for kk in msg[k].keys():
                        metric_fields[kk] = msg[k][kk]
                elif k == "beat":
                    metric_fields["inpera_topic_name"] = kafka_topic
                    # print("beat", msg[k].keys())
                    # metric_fields[k] = kafka_topic
                    for kk in msg[k].keys():
                        metric_fields[kk] = msg[k][kk]
                elif k == "message":
                    # print("message", msg[k])
                    # metrics[k] = msg[k]
                    metric_data = processMessageAttribs(msg[k])
                    metric_fields.update(metric_data)
                else:
                    metric_fields[k] = msg[k]
            except Exception as e:
                print("Error in convert2Metrics keys", e, k)
                logFile.write("Error in convert2Metrics keys" + str(e) + msg)

        metric["fields"] = metric_fields
        # check if CI name is in ci_list
        if publish2Metrics(metric_fields["ci_name"]) == 1:
            # print pretty metrics
            # print(json.dumps(metric, indent=4))
            # logFile.write(json.dumps(metric, indent=4) + "\n")
            # publish metrics to splunk
            im2s.sendMetrics2Splunk(metric, logFile)
        else:
            pass
            """
            print(
                "convert2Metrics: CI name not in list",
                metric_fields["ci_name"],
                kafka_topic,
            )
            logFile.write(
                "convert2Metrics: CI name not in list"
                + metric_fields["ci_name"]
                + kafka_topic
                + "\n"
            )
            """

    except Exception as e:
        # print("Error in convert2Metrics", e)
        logFile.write("Error in convert2Metrics" + str(e) + msg)
    return 1


def processMessageAttribs(message):
    metric_fields = {}
    for m in message.split(","):
        try:
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
        except Exception as e:
            print("Error in processMessageAttribs", e)
            logFile.write("Error in processMessageAttribs" + str(e) + message)
    return metric_fields


def publish2Metrics(ci_name):
    if ci_name in ci_list.keys():
        return 1
    else:
        return 0
