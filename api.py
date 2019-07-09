from flask import Flask, request
from flask_restful import Resource, Api ,reqparse, fields, marshal_with, marshal
from db_connector import *
import json
from flask_restful import reqparse

app = Flask(__name__)
api = Api(app)

todos = {}
class Query(Resource):
    
    #@marshal_with(resource_fields, envelope='resource')
    def get(self, table_name):
        parser = reqparse.RequestParser()
        parser.add_argument('_size', type=int, dest='limit')
        parser.add_argument('_fields', type=str, dest='fields')
        parser.add_argument('_where', type=str, dest='whereclause')
        parser.add_argument('_sort', type=str, dest='order_by')
        db_conn = ConnectorFactory.curr_connector
        args = parser.parse_args()
        print(table_name)
        print(args)
        #result = db_conn.query_table(table_name, **args)
        #print(result)
        #return json.dumps(marshal(db_conn.query_table(table_name, **args), resource_fields))
        return db_conn.query_table(table_name, **args)
    
api.add_resource(Query, '/api/<string:table_name>/')

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
    cf = ConnectorFactory
    db_conn = cf.get_or_createConnector(**presto_server)
    app.run(debug=True,host='0.0.0.0')
