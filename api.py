from flask import Flask, request
from flask_restful import Resource, Api
from db_connector import *

app = Flask(__name__)
api = Api(app)

todos = {}

class APIQuery(Resource):
    def get(self, table):
        cf = ConnectorFactory()
        db_conn = cf.get_or_createConnector(server_name="presto_yz_targetmetric_dim")
        return {table: db_conn.query_table(table)}

api.add_resource(APIQuery, '/api/query/<string:table>')

#class Flask_Rest_Server():
#    def __init__(self):
#        # load config to create db_connector
#        cf = ConnectorFactory()
#        presto_server = {
#            "server_name": "presto_yz_targetmetric_dim",
#            "connect_type": "PrestoConnector",
#            "url": {
#                "username": "hive"
#                ,"host": "hdfs01-dev.yingzi.com"
#                ,"port": 3600
#                ,"param" : "hive"
#                ,"schema": "yz_targetmetric_dim"
#            }
#        }
#        self.db_conn = cf.get_or_createConnector(**presto_server)
#        db_conn.get_table('dim_com_time')
    

    

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
    app.run(debug=True)
