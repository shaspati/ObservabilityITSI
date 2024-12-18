# import sys
from neo4j import GraphDatabase
import util as utl

uri = utl.neo_uri
user = utl.neo_user
pwd = utl.neo_pwd

driver = GraphDatabase.driver(uri=uri, auth=(user, pwd))


def creZabbixRela(sysId):
    try:
        query_old = f"""
        MATCH (n {{sys_id: '{sysId}'}}), (a:alerts {{event_sysid: '{sysId}'}})
        MERGE (n)-[r:Recived_Alert]->(a)
        RETURN r
        """
        # print(query_old)
        query = f"""
        MATCH ((n) WHERE (n:cmdb_ci_appl OR n:cmdb_ci_server OR n:cmdb_ci_dns_name OR n:cmdb_ci_application_cluster) AND n.sys_id = '{sysId}'),(a:alerts {{event_sysid: '{sysId}'}})
          MERGE (n)-[r:Recived_Alert]->(a)
        RETURN r 
        """

        # print(query)
        with driver.session() as session:
            result = session.run(query)
            # print("relupsert", result.single())
        return 1
    except Exception as e:
        print(f"An error occurred(creZabbixRela): {e},{sysId}")
        return 0


def upsertZabbixNode(data):
    success_flg = True
    try:
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
    except Exception as e:
        success_flg = False
        print(f"An error occurred(upsertZabbixNode) : {e},{data['event_sysid']}")

    if success_flg:
        sysId = data["event_sysid"]
        creZabbixRela(sysId)
        # print(result.single())


def creTeAlertRel(ci_sys_id, testId):
    try:
        # create or update relationship between CI node and alerts node
        query_old = f"""
        MATCH (n {{sys_id: '{ci_sys_id}'}}), (a:alerts {{testId: '{testId}'}})
        MERGE (n)-[r:Recived_Alert]->(a)
        RETURN r
        """
        # print(query)

        query = f"""
        MATCH ((n) WHERE (n:cmdb_ci_appl) AND n.sys_id = '{ci_sys_id}'), (a:alerts {{testId: '{testId}'}})
        MERGE (n)-[r:Recived_Alert]->(a)
        RETURN r
        """
        # print(query)

        with driver.session() as session:
            result = session.run(query)
            # print("relupsert", result.single())
    except Exception as e:
        print(f"An error occurred(creTeAlertRel): {e},{ci_sys_id},{testId}")


def getCISysId(te_id):
    # get ci sys_id based on alertid from TE2ESP_Mapping node and use to check if relationship exists
    try:
        query = f"""
        MATCH (n:TE2ESP_Mapping {{te_id: '{te_id}'}}) RETURN n.ci_sys_id
        """
        # print(query)
        with driver.session() as session:
            result = session.run(query)
            for record in result:
                sysId = record["n.ci_sys_id"]
                # print("getESP ci sysId", sysId)
    except Exception as e:
        print(f"An error occurred(getCISysId): {e},{te_id}")
        sysId = None
    return sysId


def upsertTeNode(data):
    # set key and values from alert_data dynamically
    success_flg = True
    try:
        keys = data.keys()
        query = (
            """
        MERGE (p:alerts {testId: '"""
            + data["testId"]
            + """'})
        SET """
        )
        # set key and values from alert_data dynamically
        for key in keys:
            query += f"p.{key} = '{data[key]}',"
            # remove last comma
        query = query[:-1]
        """
        RETURN p.id,p.elementId
        """
        # print(query)
        # Execute the query using a transaction
        with driver.session() as session:
            result = session.run(query)
            # print(result.single())
    except Exception as e:
        success_flg = False
        print(f"An error occurred(upsertTeNode) : {e},{data['testId']}")

    if success_flg:
        try:
            te_id = data["testId"]
            ci_sys_id = getCISysId(te_id)
            # print(ci_sys_id)
            # create or update relationship between CI node and alerts node
            if ci_sys_id:
                creTeAlertRel(ci_sys_id, te_id)
        except Exception as e:
            print(
                f"An error occurred while creating/updating relationship: {e},{data['alertId']}"
            )


if driver:
    driver.close()
