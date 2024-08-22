import json
import threading

# from kafka import KafkaConsumer
# import InperaMetricsKafka as imk
import InperaMetricsKafka as imk

# metricDataFromTopics -> InperaMetricsKafka -> InperaMetrics2Splunk , util.py, ci_list.json
kafka_topic_names = [
    "sre-sniper-logs-alln",
    "sre-logs-vif-alln",
    "sre-sniper-logs-rcdn",
    "sre-logs-vif-rcdn",
    "sre-sniper-logs-rtp",
    "sre-logs-vif-rtp",
    "sre-sniper-logs-mtv",
    "sre-logs-vif-mtv",
    "eng-inpera-bgl",
    "eng-inpera-bgl-vif",
    "eng-inpera-ads",
    "eng-inpera-ads-vif",
    "eng-inpera-rtp",
    "eng-inpera-rtp-vif",
]

kafka_topic_names = [
    "sre-sniper-logs-alln",
    "sre-logs-vif-alln",
    "sre-sniper-logs-rcdn",
    "sre-logs-vif-rcdn",
]

kafka_brokers = "173.37.82.154:9092,173.37.82.155:9092,173.37.82.156:9092,173.37.82.151:9092,173.37.82.152:9092"

thread_no = 1
for kafka_topic in kafka_topic_names:
    print("Thread starting for topic: ", kafka_topic, "Thread no: ", thread_no)
    t = threading.Thread(
        target=imk.publishMetrics2SplunkIdx, args=(kafka_topic, kafka_brokers)
    )
    t.start()
    print("Thread started for topic: ", kafka_topic, "Thread no: ", thread_no)
    thread_no += 1
