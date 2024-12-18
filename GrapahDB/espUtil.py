# Util file to retrieve the environment Variables

# Note: It is advisable to have all the below values as environment variables and then load it from there rather than having it hardcoded.

# Kafka_Config_parameters
# For more information on the kafka consumer configs, please visit this link: https://docs.confluent.io/platform/current/installation/configuration/consumer-configs.html
bootstrap_server = "chain-dev-09.cisco.com:9092,chain-dev-13.cisco.com:9092,chain-dev-15.cisco.com:9092"  # Dev Bootstrap servers used to connect the dev cluster
# bootstrap_server - for prod = chain-prd-30.cisco.com:9092,chain-prd-31.cisco.com:9092,chain-prd-32.cisco.com:9092
session_timeout_ms = 6000
auto_offset_reset = "earliest"
enable_auto_offset_store = False
sasl_username = "esp_kafka_nprd_user"  # User to be used for dev and stage
# esp_kafka_prd_user - #User to be used for prd
compression_type = "snappy"
consumer_group_Id = "esp-dev-team"  # Use the naming convention as in the example Eg: esp-dev-infra_DBTS where topic name (esp-dev-infra) is suffixed with your team name (DBTS)
topic_to_consume = "esp-dev-app"  # Topic from which you want to consume.
security_protocol = "SASL_PLAINTEXT"
sasl_mechanisms = "SCRAM-SHA-512"


# CyberArk Configs - Please do not change these parameters.
app_id = "esp-kafka-users"  # App_ID used for connecting to Users Safe
obj_name = "Operating System-WinServerLocal-cisco.com-esp_kafka_nprd_user"  # Object_name for esp_kafka_nprd_user
# obj_name for Prd user - Operating System-WinServerLocal-cisco.com-esp_kafka_prd_user
secret_timeout = 5
client_cert = "/Users/shaspati/Development/ObservabilityITSI/GrapahDB/CyberArk_Cert/espmsgdev_cert.pem"  # Set the fullpath where you have the .pem file
client_key = "/Users/shaspati/Development/ObservabilityITSI/GrapahDB/CyberArk_Cert/espmsgdev_cert.key"  # Set the fullpath where you have the .key file
