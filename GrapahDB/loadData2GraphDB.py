# import sys
from neo4j import GraphDatabase
import util as utl

uri = utl.neo_uri
user = utl.neo_user
pwd = utl.neo_pwd

driver = GraphDatabase.driver(uri=uri, auth=(user, pwd))


# CI data
def loadNodeData(className, data):
    keys = data[0].keys()
    # print(keys)
    query = f"""
    UNWIND $data AS row
    CREATE (p: {className} {{
        {', '.join([f'{key}: row.{key}' for key in keys])}
    }})
    """

    # Execute the query using a transaction
    with driver.session() as session:
        session.run(query, data=data)


# relationship data between CI nodes
def loadRelData(data, p_class_name, c_class_name):
    # Create the Cypher query dynamically based on the keys
    query = f"""
    UNWIND $data AS rel_data
    MERGE (p: {p_class_name} {{sys_id: rel_data.parent}})
    MERGE (c: {c_class_name} {{sys_id: rel_data.child}})
    with p,c,rel_data
    CALL apoc.create.relationship( p, rel_data.reltype, {{sys_id: rel_data.sys_id}}, c) yield rel return rel
    """
    # print(query)

    # Execute the query using a transaction

    with driver.session() as session:
        session.run(query, data=data)


# Zabbix alerts data
def upsertZabbixNode(data):
    # set key and values from alert_data dynamically
    keys = data.keys()
    query = (
        """
    MERGE (p:alerts {event_sysid: '"""
        + data["event_sysid"]
        + """'})
    SET """
    )
    # set key and values from alert_data dynamically
    for key in keys:
        query += f"p.{key} = '{data[key]}',"
        # remove last comma
    query = query[:-1]
    """
    RETURN p
    """
    # print(query)
    # Execute the query using a transaction
    with driver.session() as session:
        result = session.run(query)
        # print(result.single())

    sysId = data["event_sysid"]
    # create or update relationship between CI node and alerts node
    sysId = data["event_sysid"]
    query = f"""
    MATCH (n {{sys_id: '{sysId}'}}), (a:alerts {{event_sysid: '{sysId}'}})
    MERGE (n)-[r:Recived_Alert]->(a)
    RETURN r
    """
    # print(query)

    with driver.session() as session:
        result = session.run(query)
        # print(result.single())


# Thousand Eyes Alerts
def creTeAlertRel(ci_sys_id, alertId):
    # create or update relationship between CI node and alerts node
    query = f"""
    MATCH (n {{sys_id: '{ci_sys_id}'}}), (a:alerts {{alertId: '{alertId}'}})
    MERGE (n)-[r:Recived_Alert]->(a)
    RETURN r
    """
    # print(query)

    with driver.session() as session:
        result = session.run(query)
        print("relupsert", result.single())


def getCISysId(te_id):
    # get ci sys_id based on alertid from TE2ESP_Mapping node and use to check if relationship exists
    query = f"""
    MATCH (n:TE2ESP_Mapping {{te_id: '{te_id}'}}) Return n.ci_sys_id
    """
    # print(query)
    with driver.session() as session:
        result = session.run(query)
        for record in result:
            sysId = record["n.ci_sys_id"]
            print("getESP ci sysId", sysId)
    return sysId


def upsertTeNode(data):
    # set key and values from alert_data dynamically
    keys = data.keys()
    query = (
        """
    MERGE (p:alerts {alertId: '"""
        + data["alertId"]
        + """'})
    SET """
    )
    # set key and values from alert_data dynamically
    for key in keys:
        query += f"p.{key} = '{data[key]}',"
        # remove last comma
    query = query[:-1]
    """
    RETURN p
    """
    print(query)
    # Execute the query using a transaction
    with driver.session() as session:
        result = session.run(query)
        print(result.single())

    alertId = data["alertId"]
    ci_sys_id = getCISysId(data["testId"])
    print(ci_sys_id)
    # create or update relationship between CI node and alerts node
    if ci_sys_id:
        creTeAlertRel(ci_sys_id, alertId)


if driver:
    driver.close()
