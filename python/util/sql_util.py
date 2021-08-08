import sqlite3
from sqlite3 import Error
import yaml
get_Conn_str = ""
with open('./../configuration/conn_string.yaml') as yaml_data:
        prop = yaml.safe_load(yaml_data)
get_Conn_str = prop['DB_path']  


class SqlUtil:  
    def get_cur():
        con = sqlite3.connect(get_Conn_str) 
        cur = con.cursor()
        return cur 

    def data_insert(rows):
        con = sqlite3.connect(get_Conn_str) 
        cur = con.cursor()
        cur.executemany("INSERT INTO PRODUCT_TEMP (name, sku,description) VALUES (?, ?, ?);", rows)
        con.commit()
        con.close()
        
    def execute_custom_query(query):
        con = sqlite3.connect(get_Conn_str) 
        cur = con.cursor()
        res = cur.execute(query)
        con.commit()
        return res

    def close_conn():
        con = sqlite3.connect(get_Conn_str)
        con.close()
       


