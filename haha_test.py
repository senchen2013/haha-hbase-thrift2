#!/usr/bin/python
#encoding:utf-8
'''
Author: senchen2013 
Email: senchen2013@163.com
''' 

import haha

class TestHbase2Client(object):
    
    def setUp(self):
        self.hbase2Client = haha.Hbase2Client("localhost", 9090, 1000)

    def test1Put(self):
        self.hbase2Client.put('test:haha', 'row1', 'f1', 'value1')
        get = self.hbase2Client.get('test:haha', 'row1')
        assert get.get("f1", None) == "value1"
    
    def test2Delete(self):
        assert self.hbase2Client.exists('test:haha', 'row1', ['f1']) == True
        self.hbase2Client.delete('test:haha', 'row1', ['f1']) 
        assert self.hbase2Client.exists('test:haha', 'row1', ['f1']) == False
        
    
