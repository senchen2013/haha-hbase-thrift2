haha-hbase-thrift2      
    a simple hbase thrift2 client for python
        main functions such as: put, put_row, put_rows, get, get_row, get_rows, delete, delete_rows, exists 
        
   requirement:
        pip install thrift
        
   usage:
   
        import haha
        ha = haha.Hbase2Client(host, port, timeout) # timeout is ms
        ha.put('[namespace:]tablename', 'row_key', 'field', 'value')
        
for more, see haha_test.py
