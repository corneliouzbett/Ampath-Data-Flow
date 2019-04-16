# create connection to mysql database

import mysql.connector
import json

with open('../../config/config.json', 'r') as f:
    config = json.load(f)

class MysqlHelper:

    def __init__(self):
        # MySQLConfig = {'user': 'user','password': 'password','host': 
        # 'localhost','database': 'DB','raise_on_warnings': True}
        self.cnx = mysql.connector.connect(
            host= config['DATABASE_CONFIG']['host'],
            user= config['DATABASE_CONFIG']['user'],
            port= config['DATABASE_CONFIG']['port'],
            passwd= config['DATABASE_CONFIG']['password'],
            database= config['DATABASE_CONFIG']['db'],
            raise_on_warnings = True
        ) 
        self.cursor = self.cnx.cursor()

    def execute_query(self,query):
        self.cursor.execute(query)      
        for row in self.cursor:
            print(row)

# mysql_helper = MysqlHelper()
# mysql_helper.execute_query("SHOW tables")

# def connect():
#   return mysql.connector.connect(
#        host= config['DATABASE_CONFIG']['host'],
#        user= config['DATABASE_CONFIG']['user'],
#        port= config['DATABASE_CONFIG']['port'],
#        passwd= config['DATABASE_CONFIG']['password'],
#        database= config['DATABASE_CONFIG']['db']
#        raise_on_warnings = True
#    ).cursor()

#   def rollback():