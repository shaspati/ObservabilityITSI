import urllib.parse

instance_name = "ciscostage"

common_attrib = [
    "name",
    "sys_id",
    "sys_class_name",
    "operational_status",
    "support_group",
    "u_duty_pager",
    "u_monitoring_priority",
    "discovery_source",
]

classData = {
    "u_service_domain": {"attribs": ["owned_by"], "filter": "operational_statusIN6,1"},
    "u_service_category": {
        "attribs": [
            "owned_by",
            "u_chief_architect",
        ],
        "filter": "operational_statusIN6,1",
    },
    "u_it_services": {
        "attribs": [
            "owned_by",
            "assigned_to",
            "u_service_security_prime",
        ],
        "filter": "operational_statusIN6,1",
    },
    "u_it_service_offering": {
        "attribs": ["owned_by"],
        "filter": "operational_statusIN6,1",
    },
    "cmdb_ci_business_app": {
        "attribs": [
            "u_monitoring_priority",
            "u_responsibility_manager",
            "u_support_manager",
        ],
        "filter": "operational_statusIN6,1^sys_class_name=cmdb_ci_business_app",
    },
    "sn_apm_business_application_module": {
        "attribs": [
            "u_monitoring_priority",
            "u_responsibility_manager",
            "u_support_manager",
        ],
        "filter": "operational_statusIN6,1",
    },
    "u_business_database_service": {
        "attribs": [
            "u_monitoring_priority",
            "u_prod_db_name",
            "u_db_type",
            "u_erp_status",
            "assigned_to",
            "u_cisco_change_alias",
        ],
        "filter": "operational_statusIN6,1",
    },
    "u_cmdb_ci_cisco_application_instance": {
        "attribs": [
            "u_used_for",
            "u_monitoring_priority",
            "u_cisco_change_alias",
        ],
        "filter": "operational_statusIN6,1",
    },
    "cmdb_ci_appl": {
        "attribs": [
            "used_for",
            "u_monitoring_priority",
            "u_cisco_change_alias",
            "u_server as host",
        ],
        "filter": "operational_statusIN6,1",
    },
    "cmdb_ci_server": {
        "attribs": [
            "hardware_status",
            "u_monitoring_priority",
            "serial_number",
            "model_id",
            "os_version",
            "ip_address",
        ],
        "filter": "operational_statusIN6,1",
    },
    "cmdb_ci_dns_name": {
        "attribs": ["u_dns_type", "ip_address", "u_visibility"],
        "filter": "operational_statusIN6,1",
    },
    "cmdb_ci_application_cluster": {
        "attribs": [
            "u_used_for",
            "u_monitoring_priority",
            "u_cluster_classification",
        ],
        "filter": "operational_statusIN6,1",
    },
}

"""
for className in classData.keys():
    idx = "create index idx_" + className + " for (n:" + className + ") on (n.sys_id)"
    print(idx)
"""


def getClassURl(className):
    attributes = common_attrib + classData[className]["attribs"]
    attribs = ", ".join(attributes)
    attribs = urllib.parse.quote_plus(attribs)
    filter = classData[className]["filter"]
    filter = urllib.parse.quote_plus(filter)
    url = f"https://{instance_name}.service-now.com/api/now/table/{className}?sysparm_query={filter}&sysparm_display_value=true&sysparm_fields={attribs}&sysparm_orderby=sys_id&sysparm_no_count=false"
    return url


# url = getClassAttribs("cmdb_ci_dns_name")
# print(url)


def getRelURL(pclass, cclass, p_is_instance, c_is_instance):
    attribs = "sys_id,parent.sys_id,child.sys_id,type.name"
    attribs = urllib.parse.quote_plus(attribs)
    if p_is_instance == True and c_is_instance == True:
        filter = f"parent.sys_class_nameINSTANCEOF{pclass}^child.sys_class_nameINSTANCEOF{cclass}"
    elif p_is_instance == True and c_is_instance == False:
        filter = (
            f"parent.sys_class_nameINSTANCEOF{pclass}^child.sys_class_name={cclass}"
        )
    elif p_is_instance == False and c_is_instance == True:
        filter = (
            f"parent.sys_class_name={pclass}^child.sys_class_nameINSTANCEOF{cclass}"
        )
    else:
        filter = f"parent.sys_class_name={pclass}^child.sys_class_name={cclass}"
    filter = urllib.parse.quote_plus(filter)
    url = f"https://{instance_name}.service-now.com/api/now/table/cmdb_rel_ci?sysparm_query={filter}&sysparm_fields={attribs}&sysparm_orderby=sys_id&sysparm_no_count=false"
    # url = "https://ciscostage.service-now.com/api/now/table/cmdb_rel_ci?sysparm_query=parent.sys_class_nameINSTANCEOFcmdb_ci_appl%5Echild.sys_class_nameINSTANCEOFcmdb_ci_server%5Eparent.operational_statusIN6%2C1&sysparm_fields=sys_id%2Cparent.sys_id%2Cchild.sys_id%2Ctype.name"
    # print(url)
    return url


# getRelQuery("u_service_domain", "u_service_category")
