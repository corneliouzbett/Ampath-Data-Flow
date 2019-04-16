from spark_commons import commons

import sys
import json

#with open('../../config/config.json') as f:
with open('/home/corneliouzbett/Documents/projects/Ampath-Data-Flow/config/config.json') as f:
    config = json.load(f)

class BaseAmrs(object):
    
    def __init__(self,sql_context):
        self.sql_context = sql_context

    # creates dataframes
    def create_dataframe(self, df_name):
        return self.sql_context.read\
            .format('jdbc')\
            .options(
                url = config['DATABASE_CONFIG']['url'],
                driver = config['DATABASE_CONFIG']['driver'],
                user = config['DATABASE_CONFIG']['user'],
                password = config['DATABASE_CONFIG']['password'],
                dbtable = config[df_name]['dbtable'],
                partitionColumn = config[df_name]['partitionColumn'],
                numPartitions = config[df_name]['numPartitions'],
                fetchsize = config[df_name]['fetchsize'],
                lowerBound = config[df_name]['lowerBound'],
                upperBound = config[df_name]['upperBound'] )\
            .load()