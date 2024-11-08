from neo4j import GraphDatabase
import espDataMart as dm
import time

# Establish a connection to the Neo4j database
driver = GraphDatabase.driver(
    "neo4j://10.114.248.150:7687", auth=("neo4j", "NeoAdmin1!")
)
# this actually table names
p_class_name = "cmdb_ci_appl"
# p_class_name = "sn_apm_business_ication_module"
# c_class_name = "sn_apm_business_ication_module"
c_class_name = "cmdb_ci_appl"

print("started getting data from db class_name: ", time.ctime())
# data = dm.getRelData(p_class_name, c_class_name)
data = dm.getRelDataEp2Ep(p_class_name, c_class_name)
# split data into chuck of 20000 records
print("finished getting data from db class_name: ", len(data), time.ctime())
data = [data[i : i + 10000] for i in range(0, len(data), 10000)]
# c_class_name = "sn_apm_business_application_module"


def load_relData(data, reltype):
    # Create the Cypher query dynamically based on the keys
    driver = GraphDatabase.driver(
        "neo4j://10.114.248.150:7687", auth=("neo4j", "NeoAdmin1!")
    )
    query = f"""
    UNWIND $data AS rel_data
    MERGE (p: {p_class_name} {{sys_id: rel_data.parent}})
    MERGE (c: {c_class_name} {{sys_id: rel_data.child}})
    MERGE (p)-[r:{reltype} {{sys_id: rel_data.sys_id}}]->(c)
    """
    # print(query)
    # with p,c,rel_data
    # CALL apoc.create.relationship( p, rel_data.reltype, {{sys_id: rel_data.sys_id}}, c) yield rel return rel

    # Execute the query using a transaction

    with driver.session() as session:
        session.run(query, data=data)
    driver.close()


def loadRelData(data):
    # Create the Cypher query dynamically based on the keys
    driver = GraphDatabase.driver(
        "neo4j://10.114.248.150:7687", auth=("neo4j", "NeoAdmin1!")
    )
    query = f"""
    UNWIND $data AS rel_data
    MERGE (p: {p_class_name} {{sys_id: rel_data.parent}})
    MERGE (c: {c_class_name} {{sys_id: rel_data.child}})
    with p,c,rel_data
    CALL apoc.create.relationship( p, rel_data.reltype, {{sys_id: rel_data.sys_id}}, c) yield rel return rel
    """
    print(query)
    # with p,c,rel_data
    # CALL apoc.create.relationship( p, rel_data.reltype, {{sys_id: rel_data.sys_id}}, c) yield rel return rel

    # Execute the query using a transaction

    with driver.session() as session:
        session.run(query, data=data)
    driver.close()


for chunk in data:
    print(f"Processing {len(chunk)} records", chunk[0:10])
    reltype = "DependsOn"
    load_relData(chunk, reltype)
    # loadRelData(chunk)
