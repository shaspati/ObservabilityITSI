import requests
import sys, time
import classAttribMapping as cam

sys.path.append("/Users/shaspati/Development/ObservabilityITSI/esp")
import util as utl
import espToken as t  # type: ignore
import loadData2GraphDB as gd

token = t.getToken()
headers = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Authorization": token,
}


def getESP4Data(className):
    url = cam.getClassURl(className)
    # print(url)
    offset = 0
    limit = 10000

    while True:
        furl = url + f"&sysparm_limit={limit}&sysparm_offset={offset}"
        # print(furl)
        response = requests.get(
            furl,
            headers=headers,
            # params={"sysparm_limit": limit, "sysparm_offset": offset},
        )

        data = response.json().get("result")
        # if key is dict get display value from it and assing to key
        # print(data)
        if not data:
            break  # Exit loop if there are no more records
        if offset == 0:
            print("Total Count:", response.headers["X-Total-Count"])

        for item in data:
            for key in item.keys():
                if isinstance(item[key], dict):
                    item[key] = item[key].get("display_value")
        # 1st 10 records
        # print(data[0:10])
        gd.loadNodeData(className, data)
        # Process the data here
        print(f"Retrieved {len(data)} records, offset: {offset}")
        # get header data and print X-Total-Count
        offset += limit  # Increment offset to move to the next page


def getMonitoringCollectors4TE():
    url = f"https://{utl.instance_name}.service-now.com/api/now/table/u_monitoring_collectors?sysparm_query=u_monitoring_system=e1c9730bdb0aa410fe8da90b8a9619d8&sysparm_fields=sys_id,u_host.sys_id,u_monitoring_link,u_monitoring_priority&sysparm_orderby=sys_id&sysparm_no_count=false"

    response = requests.get(
        url,
        headers=headers,
    )
    data = response.json().get("result")
    # print(data[0:10])
    # rename u_host.sys_id to ci_sys_id, split u_monitoring_link '=' and get 2nd part and assign to u_monitoring_link
    for item in data:
        item["ci_sys_id"] = item.pop("u_host.sys_id")
        item["te_id"] = item["u_monitoring_link"].split("=")[1]
    # print(data[0:10])
    print("Total Count:", response.headers["X-Total-Count"], len(data))
    gd.loadNodeData("TE2ESP_Mapping", data)


def getRelData(p_class_name, c_class_name, p_is_instance=False, c_is_instance=False):
    url = cam.getRelURL(p_class_name, c_class_name, p_is_instance, c_is_instance)
    print(url)
    print(
        f"Processing relationship data for {p_class_name} and {c_class_name}",
        time.ctime(),
    )
    offset = 0
    limit = 10000
    while True:
        furl = url + f"&sysparm_limit={limit}&sysparm_offset={offset}"
        print(furl)
        response = requests.get(
            furl,
            headers=headers,
            # params={"sysparm_limit": limit, "sysparm_offset": offset},
        )

        data = response.json().get("result")
        if offset == 0:
            print("Total Count:", response.headers["X-Total-Count"])

        if not data:
            break  # Exit loop if there are no more records
        # print(data[0])
        print(len(data))
        # replace data keys parent.sys_id to parent and child.sys_id to child and type.name to reltype
        for item in data:
            # print(item)
            try:
                item["parent"] = item.pop("parent.sys_id")
                item["child"] = item.pop("child.sys_id")
                item["reltype"] = item.pop("type.name")
            except Exception as e:
                print(f"Key error: {e} in item: {item}")

        # load data into graph database using loadRelData
        gd.loadRelData(data, p_class_name, c_class_name)
        offset += limit  # Increment offset to move to the next page
        print(f"Retrieved {len(data)} records, offset: {offset}")


def getRelData2file(
    p_class_name, c_class_name, p_is_instance=False, c_is_instance=False
):
    url = cam.getRelURL(p_class_name, c_class_name, p_is_instance, c_is_instance)
    print(url)
    print(
        f"Processing relationship data for {p_class_name} and {c_class_name}",
        time.ctime(),
    )
    offset = 0
    limit = 10000
    final_obj = []
    while True:
        furl = url + f"&sysparm_limit={limit}&sysparm_offset={offset}"
        print(furl)
        response = requests.get(
            furl,
            headers=headers,
            # params={"sysparm_limit": limit, "sysparm_offset": offset},
        )

        data = response.json().get("result")
        if offset == 0:
            print("Total Count:", response.headers["X-Total-Count"])

        # append data to file in json format file name rel_ep_server.json
        if not data:
            print("Writing data to file")
            # write final_obj to file using open file
            with open(f"rel_ep_server.json", "w") as f:
                f.write(str(final_obj))

            break  # Exit loop if there are no more records
        final_obj = final_obj + data
        print(len(data))
        # print(data[0])
        # replace data keys parent.sys_id to parent and child.sys_id to child and type.name to reltype
        offset += limit  # Increment offset to move to the next page
        print(f"Retrieved {len(data)} records, offset: {offset}")


classList = [
    "u_service_domain",
    "u_service_category",
    "u_it_services",
    "u_it_service_offering",
    "cmdb_ci_business_app",
    "sn_apm_business_application_module",
    "u_business_database_service",
    "u_cmdb_ci_cisco_application_instance",
    "cmdb_ci_appl",
    "cmdb_ci_application_cluster",
    "cmdb_ci_server",
    "cmdb_ci_dns_name",
]
# load data for 1st class
for className in classList[5:6]:
    print(f"Processing data for {className}", time.ctime())
    getESP4Data(className)

# getESP4Data("u_monitoring_collectors")
getMonitoringCollectors4TE()

# load relationship data
# getRelData("u_service_domain", "u_service_category")
# getRelData("u_service_category", "u_it_services")
# getRelData("u_it_services", "u_it_service_offering")
# getRelData("u_it_service_offering", "cmdb_ci_business_app")
# getRelData("cmdb_ci_business_app", "sn_apm_business_application_module")
# getRelData("cmdb_ci_business_app", "u_cmdb_ci_cisco_application_instance")
# getRelData("sn_apm_business_application_module", "u_cmdb_ci_cisco_application_instance")
# getRelData("u_business_database_service", "u_cmdb_ci_cisco_application_instance")
# getRelData("u_cmdb_ci_cisco_application_instance", "u_cmdb_ci_cisco_application_instance")
# getRelData("u_cmdb_ci_cisco_application_instance", "cmdb_ci_application_cluster")
# getRelData("cmdb_ci_application_cluster", "cmdb_ci_application_cluster")
# getRelData("u_cmdb_ci_cisco_application_instance", "cmdb_ci_appl", False, True)
# getRelData("cmdb_ci_application_cluster", "cmdb_ci_appl", False, True)
# getRelData("cmdb_ci_appl", "cmdb_ci_application_cluster", True, False)
# getRelData("cmdb_ci_appl", "cmdb_ci_appl", True, True)  # deleted and recreate
# getRelData2file("cmdb_ci_appl", "cmdb_ci_server", True, True)
# getRelData("cmdb_ci_appl", "cmdb_ci_dns_name", True, False)
