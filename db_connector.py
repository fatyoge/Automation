from sqlalchemy import *
from sqlalchemy.engine import create_engine
from sqlalchemy.schema import *
import logging
from pyhive import hive
from sqlalchemy import desc, asc 
from collections import OrderedDict 
import json
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
from flask_restful import marshal, fields
from sqlalchemy.sql import sqltypes
import re
from utils import SQLFormator

class Connector():
    #url = 'driver://username:password@host:port/database'
    def __init__(self):
        self.engine = None
        self.connect_url = {}
        self.table_obj = {}
        self.type_map = {
            sqltypes.String : fields.String,
            sqltypes.Integer : fields.Integer,
            sqltypes.DateTime : fields.DateTime,
            sqltypes.Date : fields.String,
            sqltypes.BigInteger : fields.Integer,
            sqltypes.Float : fields.Float,
            sqltypes.Boolean : fields.Boolean,
            sqltypes.NullType: fields.String,
        }
        

    def set_addr(self, url):
        for k in url:
            self.connect_url[k]=url[k]
    
    def get_engine(self):
        pass

    def exec_sql(self):
        pass
    
    def get_table(self, table_name):
        pass
    
    def query_table(self, table):
        if isinstance(table, str):
            table = self.get_table(table)
        return select([func.count('*')], from_obj=table).scalar()
    
class HiveSqlaConnector(Connector):
    def __init__(self):
        super().__init__()
        self.connect_url['driver'] = 'hive'
        pass
    
    def get_engine(self):
        url = '{}://{}@{}:{}/{}'.format(
                  self.connect_url['driver']
                , self.connect_url['username']
                , self.connect_url['host']
                , self.connect_url['port']
                , self.connect_url['param']
                )
        self.engine = create_engine(url)
        return self.engine

class HiveDBApiConnector(Connector):
    def __init__(self):
        super().__init__()
        pass
    
    def get_engine(self):
        self.engine = hive.connect(
              host=self.connect_url['host']
            , port=self.connect_url['port']
            , username=self.connect_url['username']).cursor()
        return self.engine

class PrestoConnector(Connector):
    def __init__(self):
        super().__init__()
        self.connect_url['driver'] = 'presto'
        self.engine = {}
        pass
    
    def get_engine(self, schema='default'):
        if schema in self.engine:
            return self.engine[schema]
        #url = 'driver://username@host:port/param/schema'
        url = '{}://{}@{}:{}/{}/{}'.format(
                  self.connect_url['driver']
                , self.connect_url['username']
                , self.connect_url['host']
                , self.connect_url['port']
                , self.connect_url['param']
                , schema
                )
        self.engine[schema] = create_engine(url)
        logging.info('create engine success!')
        return self.engine[schema]
    
    def get_table(self, table_name):
        logging.info('Start to create orm object: {}'.format(table_name))
        #print('Start to create orm object: {}'.format(table_name))
        table_params = table_name.split('.')
        table_name = table_params[-1]
        dbschema = table_params[0] if len(table_params) == 2 else 'default'
        table_orm = Table(table_name, MetaData(bind=self.get_engine(dbschema)), autoload=True)
        if table_name not in self.table_obj:
            self.table_obj[table_name] = table_orm
        return table_orm

    def query_table(self, table, fields=None, whereclause=None, order_by=None, offset=None, group_by=None, limit=None, **kwargs):
        print('start to query_table： {}'.format(table))
        query_args = OrderedDict()
        # Get table ORM object
        if isinstance(table, str):
            table = self.get_table(table)
        query_args['from_obj'] = table

        resource_type = OrderedDict()
        
        #if fields is None:
        #    if group_by is None:
        #        fields = table.alias().columns.keys()
        #    else:
        #        fields = group_by.split(',')
        #        fields.append(func.count('*'))
        #else:
        #    fields = fields.split(',')
#
        #cols = []
        #for f in fields:
        #    regs = re.findall(r'[^()]+',f)
        #    if len(regs) == 1:
        #        cols.append(f)
        #    elif len(regs) == 2 and regs[0] in self.func_map:
        #        col = regs[1]
        #        if col in table.columns:
        #            col = table.columns[col]
        #        cols.append(self.func_map[regs[0]](col))
        query_args['columns'] = SQLFormator.selectTransform(fields, table)

        if group_by is not None:
            query_args['group_by'] = group_by.split(',')

        if order_by is not None:
            query_args['order_by'] = [desc(x[1:]) if x[0]=='-' else asc(x) for x in order_by.split(',')]
                
        if whereclause is not None:
            query_args['whereclause'] = SQLFormator.whereTransform(whereclause, table)
        
        if limit is not None:
            query_args['limit'] = limit
        else:
            query_args['limit'] = 1
        #print('=======================query_args=========================')
        print(query_args)
        #return select(**query_args).scalar()
        query = select(**query_args)
        for c in query.columns:
            print(c)
            if str(c) in table.alias().columns.keys():
                resource_type[str(c)] = self.type_map[type(table.alias().columns[str(c)].type)]
            elif type(c.type) in self.type_map:
                resource_type[str(c)] = self.type_map[type(c.type)]
            #else:
            #    resource_type[str(c)] = self.type_map[sqltypes.Float]
        return json.dumps(marshal(select(**query_args).execute().fetchall(), resource_type))

def singleton(cls):
    instance = cls()
    instance.__call__ = lambda: instance
    return instance

@singleton
class ConnectorFactory:
    def __init__(self):
        self.connector_list = {}
        self.connectorFactory = {
            'PrestoConnector': PrestoConnector,
            'HiveSqlaConnector': HiveSqlaConnector,
            'HiveDBApiConnector': HiveDBApiConnector
        }
        self.curr_connector = None

    def get_or_createConnector(self, url=None, server_name='TempConnector'
            , connect_type='PrestoConnector'):
        if url is None:
            print('Please input the right host addr.')
            return 

        if server_name in self.connector_list:
            return self.connector_list[server_name]

        connectorIns = self.connectorFactory[connect_type]()
        connectorIns.set_addr(url)
        #connectorIns.get_engine()

        self.connector_list[server_name] = connectorIns
        if self.curr_connector is None:
            self.curr_connector = connectorIns
        return connectorIns
