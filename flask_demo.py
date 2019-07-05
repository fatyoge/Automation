from db_connector import *

if __name__ == '__main__':
    presto_server = {
    "server_name": "presto_yz_targetmetric_dim",
    "connect_type": "PrestoConnector",
    "url": {
        "username": "hive"
        ,"host": "hdfs01-dev.yingzi.com"
        ,"port": 3600
        ,"param" : "hive"
        ,"schema": "yz_targetmetric_dim"
        }
    }
    cf = ConnectorFactory()
    db_conn = cf.get_or_createConnector(**presto_server)
    qt = db_conn.get_table('yz_targetmetric_dim.dim_com_time')
    db_conn.query_table(qt)
    #db_conn2 = HiveSqlaConnector()
    #hive_url = {
    #    'username': 'yarn'
    #    ,'host': 'hdfs03-dev.yingzi.com'
    #    ,'port': 10000
    #    ,'param' : 'default?auth=NONE'
    #}
    #db_conn2.set_addr(**hive_url)
    #engine = db_conn2.get_engine()
    #cursor = engine.execute('show tables')
    #print(cursor.fetchall())


    #db_conn = HiveDBApiConnector()
    #hive_url = {
    #    'username': 'yarn'
    #    ,'host': 'hdfs03-dev.yingzi.com'
    #    ,'port': 10000
    #}
    #db_conn.set_addr(**hive_url)
    #engine = db_conn.get_engine()
    #engine.execute('show tables')
    #print(engine.fetchall())
