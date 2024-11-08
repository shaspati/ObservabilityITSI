# import sys
from neo4j import GraphDatabase
import util as utl

uri = utl.uri
user = utl.user
pwd = utl.pwd

driver = GraphDatabase.driver(uri=uri, auth=(user, pwd))


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


def getQuery(obj):
    query = "CREATE (:alerts {"
    for key in obj:
        query += key + ": $" + key + ","
    query = query[:-1] + "})"
    print(query)
    return query


def upsertNode(data):
    # set key and values from alert_data dynamically
    upd_keys = []
    for key in data.keys():
        if key.startswith("@"):
            upd_keys.append(key)

    for key in upd_keys:
        del data[key]

    data["name"] = data["event_entity_name"]
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
    MERGE (n)-[r:RecivedAlert]->(a)
    RETURN r
    """
    # print(query)

    with driver.session() as session:
        result = session.run(query)
        # print(result.single())


if driver:
    driver.close()
