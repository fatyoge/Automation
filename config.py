

class SETTING:
    server_list = {
        "presto": {
            "connect_type": "PrestoConnector",
            "url": {
                "username": "hive"
                ,"host": "hdfs01-dev.yingzi.com"
                ,"port": 3600
                ,"param" : "hive"
                ,"schema": "default"
            },
            "table_whitelist":  [
                "yz_targetmetric_dim.dim_com_time",
                "yz_targetmetric_brc.ods_bc_sys_config",
            ]
        },
        "hive": {
            "connect_type": "HiveSqlaConnector",
            "url": {
                "username": "yarn"
                ,"host": "hdfs03-dev.yingzi.com"
                ,"port": 10000
                ,"schema": "default"
                ,"param" : "auth=NONE"
            },
            "table_whitelist":  [
                "yz_targetmetric_dim.dim_com_time",
                "yz_targetmetric_brc.ods_bc_sys_config",
            ]
        },
#        "hiveapi": {
#            "connect_type": "HiveDBApiConnector",
#            "url": {
#                "username": "yarn"
#                ,"host": "hdfs03-dev.yingzi.com"
#                ,"port": 10000
#                ,"database": "default"
#                ,"auth" : "NONE"
#            },
#            "table_whitelist":  [
#                "yz_targetmetric_dim.dim_com_time",
#                "yz_targetmetric_brc.ods_bc_sys_config",
#            ]
#        },
    }
