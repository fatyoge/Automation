from sqlalchemy import *
from sqlalchemy.engine import create_engine
from sqlalchemy.schema import *
import logging
from pyhive import hive

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')


class Connector():
    #url = 'driver://username:password@host:port/database'
    def __init__(self):
        self.engine = None
        self.connect_url = {}
        self.table_obj = {}

    def set_addr(self, **kwargs):
        for k in kwargs:
            self.connect_url[k]=kwargs[k]
    
    def get_engine(self):
        pass

    def exec_sql(self):
        pass
    
    def get_table(self, table_name):
        logging.info('Start to create orm object: {}'.format(table_name))
        table_orm = Table(table_name, MetaData(bind=self.engine), autoload=True)
        if table_name not in self.table_obj:
            self.table_obj[table_name] = table_orm
        return table_orm
    
    def query_table(self, table):
        if type(table) is 'str':
            table = self.get_table(table)
        print(select([func.count('*')], from_obj=table).scalar())
    
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
        return self.engine[schema]
    
    def get_table(self, table_name):
        logging.info('Start to create orm object: {}'.format(table_name))
        
        table_params = table_name.split('.')
        table_name = table_params[-1]
        dbschema = table_params[0] if len(table_params) == 2 else 'default'
        table_orm = Table(table_name, MetaData(bind=self.get_engine(dbschema)), autoload=True)
        if table_name not in self.table_obj:
            self.table_obj[table_name] = table_orm
        return table_orm


class ConnectorFactory():
    def __init__(self):
        self.connector_list = {}
        self.connectorFactory = {
            'PrestoConnector': PrestoConnector,
            'HiveSqlaConnector': HiveSqlaConnector,
            'HiveDBApiConnector': HiveDBApiConnector
        }

    def get_or_createConnector(self, **kwargs):
        if kwargs['server_name'] in self.connector_list:
            return self.connector_list[kwargs['server_name']]
        connectorIns = self.connectorFactory[kwargs['connect_type']]()
        connectorIns.set_addr(**kwargs['url'])
        self.connector_list[kwargs['server_name']] = connectorIns
        return connectorIns
