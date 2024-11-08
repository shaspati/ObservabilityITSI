from dotenv import load_dotenv
import os

load_dotenv()
pwd = os.getenv("pwd")
user = os.getenv("usr")
uri = os.getenv("uri")
bootstrap_servers = os.getenv("bootstrap_servers")
group_id = os.getenv("group_id")
security_protocol = os.getenv("security_protocol")
sasl_mechanism = os.getenv("sasl_mechanism")
sasl_plain_username = os.getenv("sasl_plain_username")
sasl_plain_password = os.getenv("sasl_plain_password")
topic_name = os.getenv("topic_name")
