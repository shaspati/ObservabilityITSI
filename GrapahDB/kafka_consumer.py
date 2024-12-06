from kafka import KafkaConsumer
import util as utl


def getConsumer():
    consumer = KafkaConsumer(
        bootstrap_servers=utl.bootstrap_servers,
        group_id=utl.group_id,
        security_protocol=utl.security_protocol,
        sasl_mechanism=utl.sasl_mechanism,
        sasl_plain_username=utl.sasl_plain_username,
        sasl_plain_password=utl.sasl_plain_password,
    )
    return consumer
