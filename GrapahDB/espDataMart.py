# import esp.util as utl
# import util from directory esp in same parent directory
import sys

sys.path.append("/Users/shaspati/Development/ObservabilityITSI/esp")

import util as utl
import oracledb
import espClassMapping as qry


# import queries as qry

conn = oracledb.connect(user=utl.dbuser, password=utl.db_password, dsn=utl.dsn)
cur = conn.cursor()


# Custom function to convert rows to a dictionary
def rows_as_dict(cursor):
    # convert keys to lower case
    columns = [col[0].lower() for col in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def getData4Class(class_name):
    sql = qry.getClassAttribs(class_name)
    # sql = "select * from u_service_domain"
    print(sql)
    cur.execute(sql)

    # print data in the tablecl

    res = rows_as_dict(cur)
    # print(res)
    return res


# getData4Class("u_cmdb_ci_cisco_application_instance")


def getRelData(pclass, cclass):
    # convery array into comma separated string with double quotes
    # sysids = ",".join([f"'{x}'" for x in sysids])
    sql = (
        "select rel.sys_id, rel.parent,rel.child,regexp_replace(regexp_substr(rel.dv_type,'^[^:]+'),' ','') as relType,p.sys_class_name as pClassName,c.sys_class_name as cClassName from cmdb_rel_ci rel, "
        + pclass
        + " p,"
        + cclass
        + " c where p.sys_id=rel.parent and c.sys_id = rel.child  "
    )
    print(sql)
    cur.execute(sql)
    res = rows_as_dict(cur)
    # print(res)
    return res


def getRelDataEp2Ep(pclass, cclass):
    # convery array into comma separated string with double quotes
    # sysids = ",".join([f"'{x}'" for x in sysids])
    sql = (
        "select rel.sys_id, rel.parent,rel.child,rel.dv_type as relType,p.sys_class_name as pClassName,c.sys_class_name as cClassName from cmdb_rel_ci rel,cmdb_ci_appl p,cmdb_ci_appl c "
        + " where p.sys_id=rel.parent and c.sys_id = rel.child  and "
        + " p.u_application_instance is not null and c.u_application_instance is not null and p.operational_status in (1,6) and c.operational_status in (1,6)"
    )
    print(sql)
    cur.execute(sql)
    res = rows_as_dict(cur)
    # print(res)
    return res


# getRelData("cmdb_ci_business_app", "u_cmdb_ci_cisco_application_instance")
