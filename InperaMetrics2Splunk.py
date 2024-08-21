import requests
import json


def sendMetrics2Splunk(metric):

    url = "https://sra-index-rtp-p-04:8088/services/collector"

    payload = json.dumps(metric)

    headers = {
        "Authorization": "Splunk 0412c3c5-5dac-4e96-8237-8d0c68d58f54",
        "Content-Type": "application/json",
    }

    response = requests.request(
        "POST", url, headers=headers, data=payload, verify=False
    )

    print(response.text)
