#!/usr/bin/python
#encoding:utf-8
'''
Author: senchen2013
Email: senchen2013@163.com
''' 
import sys
reload(sys)
sys.setdefaultencoding("utf-8")

import os.path
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "lib"))

from collections import defaultdict

from thrift.transport import TTransport
from thrift.transport import TSocket
from thrift.transport import THttpClient
from thrift.protocol import TBinaryProtocol

from hbase import THBaseService
from hbase.ttypes import *

PUT_BUFFER_SIZE = 10

class NotColumnValuePairException(Exception):
    pass

class NotColumnsListException(Exception):
    pass

class Hbase2Client(object):
    
    def __init__(self, host, port, timeout = 100): 
        '''
            timeout: ms
        '''
        socket = TSocket.TSocket(host, port)
        socket.setTimeout(timeout)
        self.transport = TTransport.TBufferedTransport(socket)
        protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
        self.client = THBaseService.Client(protocol)
        self.transport.open()

    def __del__(self):
        self.transport.close()
    
    def __getColumnValues(self, family, columnValuePair = {}):
        return [TColumnValue(family = family, qualifier=k, value=v) for k, v in columnValuePair.items()]

    def __getTPut(self, row, columnValuePair = {}, family = 'fa'):
        tcvs = self.__getColumnValues(family, columnValuePair)
        return TPut(row, tcvs)
    
    def put_rows(self, table, row_columnValuePair = {}, family = 'fa'):
        '''put multiple fields'''
        tputBuffers = []
        for row, columnValuePair in row_columnValuePair.items():
            if not isinstance(columnValuePair, dict):
                raise NotColumnValuePairException()
                
            tputBuffers.append(self.__getTPut(row, columnValuePair, family))
            if len(tputBuffers) > PUT_BUFFER_SIZE:
                self.client.putMultiple(table, tputBuffers)
                tputBuffers = []
        else:
            if tputBuffers:
                self.client.putMultiple(table, tputBuffers)

    def put(self, table, row, column, value, family = 'fa'):
        self.put_rows(table, {row:{column:value}}, family) 

    def put_row(self, table, row, columnValuePair = {}, family = 'fa'):
        '''
            columnValuePair: {"f1":"value1", "f2":"value2"}
        '''
        self.put_rows(table, {row:columnValuePair}, family) 

    def __getTColumns(self, columns, family):
        if columns:
            return [TColumn(family, column) for column in columns]
        return None
   
    def get_rows(self, table, row_columns = {}, family = 'fa'):
        '''
            row_columns: {"row1": ["f1", "f2", "f3"], "row2":["f1", "f2", "f3"]}
        '''
        tgets = [TGet(row, self.__getTColumns(columns, family)) for row, columns in row_columns.items()] 
        tmp_results = self.client.getMultiple(table, tgets)
        if tmp_results:
            result = defaultdict(dict)
            for tmp_result in tmp_results:
                row = tmp_result.row
                for tr in tmp_result.columnValues:
                    result[tmp_result.row][tr.qualifier] = tr.value
            
            return dict(result) 
        return {}

    def get(self, table, row, columns = [], family = 'fa'):
        res = self.get_rows(table, {row:columns}, family)
        if res:
            return res.values()[0] # 删除row 
        return res

    def delete_rows(self, table, row_columns = {}, family = 'fa'):
        tdeletes = [TDelete(row, self.__getTColumns(columns, family)) for row, columns in row_columns.items()]    
        self.client.deleteMultiple(table, tdeletes) 
    
    def delete(self, table, row, columns = [], family = 'fa'):
        self.delete_rows(table, {row: columns}, family)

    def exists(self, table, row, columns = [], family = 'fa'):
        tget = TGet(row, self.__getTColumns(columns, family))
        return self.client.exists(table, tget)
