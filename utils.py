import re
from sqlalchemy import *
from sqlalchemy.sql import sqltypes

def singleton(cls):
    instance = cls()
    instance.__call__ = lambda: instance
    return instance

@singleton
class SQLFormator:
    string_type = [
        sqltypes.String,
        sqltypes.DateTime,
        sqltypes.Date,
    ]
    operator_map = {
        'eq':'=',
        'ne':'!=',
        'gt':'>',
        'gte':'>=',
        'lt':'<',
        'lte':'<=',
    }
    func_map = {
            'sum' : func.sum,
            'count': func.count,
            'max' : func.max,
            'min' : func.min,
            'avg' : func.avg,
            'median' : func.median,
        }
    @classmethod
    def _whereSingleTransform(cls, txt, table):
        ops = ['~or','~and','~xor']
        if str(txt).strip() in ops:
            return " {} ".format(str(txt).strip().replace('~',''))

        cond = txt.split(',')
        col = cond[0]
        oper = cls.operator_map[cond[1]]
        value = cond[2]
        if col in table.alias().columns and type(table.alias().columns[col].type) in cls.string_type:
            value = "'{}'".format(value)
        return "{} {} {}".format(cond[0], oper, value)
    
    @classmethod
    def whereTransform(cls, txt, table):
        wheres = re.findall(r'[^()]+',txt)
        for subwhere in wheres:
            tmp = cls._whereSingleTransform(subwhere, table)
            txt = str(txt).replace(subwhere,tmp,1)
        return txt

    @classmethod
    def selectTransform(cls, fields, table):
        cols = []
        if fields is None:
            cols = table.alias().columns.keys()
        else:
            fields = fields.split(',')
            for field in fields:
                cols.append(cls._selectSingleTransform(field, table))
        return cols
    
    @classmethod
    def _selectSingleTransform(cls, field, table):
        regs = re.findall(r'[^()]+',field)
        if len(regs) == 1:
            return field
        elif len(regs) == 2 and regs[0] in cls.func_map:
            col = regs[1]
            if col in table.columns:
                col = table.columns[col]
            return cls.func_map[regs[0]](col)
