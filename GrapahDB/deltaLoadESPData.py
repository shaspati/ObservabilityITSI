from loadData2GraphDB import updNodeData, delNodeData

# read data from kafka and load into graph database
# Mapping of class names to their respective graph database fields and corresponding message fields
class_field_mapping = {
    "cmdb_ci_appl": {
        "name": {"column": "name", "value_field": "value"},
        "sys_id": {"column": "sys_id", "value_field": "value"},
        "sys_class_name": {"column": "sys_class_name", "value_field": "value"},
        "operational_status": {
            "column": "u_operational_status",
            "value_field": "display_value",
        },
        "support_group": {"column": "support_group", "value_field": "display_value"},
        "duty_pager": {"column": "u_duty_pager", "value_field": "display_value"},
        "monitoring_priority": {
            "column": "u_monitoring_priority",
            "value_field": "value",
        },
        "discovery_source": {"column": "discovery_source", "value_field": "value"},
        "used_for": {"column": "used_for", "value_field": "value"},
        "server": {"column": "u_server", "value_field": "display_value"},
    }
}


def convert_kafka_msg_to_graph_obj(message, class_name):
    class_mapping = class_field_mapping[class_name]
    graph_obj = {}
    for graph_key in class_mapping.keys():
        column_name = class_mapping[graph_key]["column"]
        value_field = class_mapping[graph_key]["value_field"]
        graph_obj[graph_key] = message[column_name][value_field]

    # print(graph_obj)
    if graph_obj["operational_status"] == "Retired":
        delNodeData(class_name, graph_obj)
    else:
        updNodeData(class_name, graph_obj)

    return graph_obj


data_smg = {
    "name": "csae-prod@cd-csae-tps-data-api-rcdn-stg.cisco.com",
    "sys_id": "6f87cfeedbf30d103aa4f99468961920",
    "sys_class_name": "cmdb_ci_appl",
    "operational_status": "Retired",
    "support_group": "cdtoolschain_support_L1",
    "duty_pager": "cd-toolschain-duty",
    "monitoring_priority": "7",
    "discovery_source": "CAE",
    "used_for": "Production",
    "server": "cd-csae-tps-data-api-rcdn-stg.cisco.com",
}

obj = delNodeData("cmdb_ci_appl", data_smg)

# print(obj)
