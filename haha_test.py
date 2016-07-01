#!/usr/bin/python
#encoding:utf-8
'''
Author: senchen2013 
Email: senchen2013@163.com
''' 
import sys
reload(sys)
sys.setdefaultencoding("utf-8")

import haha

class TestHbase2Client(object):
    
    def setUp(self):
        self.hbase2Client = haha.Hbase2Client("bhd02", 9090, 1000)

    def testPut(self):
        self.hbase2Client.put('test:haha', 'row1', 'f1', 'value1')
        get = self.hbase2Client.get('test:haha', 'row1')
        assert get.get("f1", None) == "value1"
    
