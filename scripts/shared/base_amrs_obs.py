import sys
import json

#with open('../../config/config.json') as f:
with open('/home/corneliouzbett/Documents/projects/Ampath-Data-Flow/config/config.json') as f:
    config = json.load(f)


class AmrsObs(object):
    
    def __init__(self,sql_context):
        self.sql_context = sql_context

    # create obs dataframe
    def generate_obs_dataframe(self):
        return self.sql_context.read\
            .format('jdbc')\
            .options(
                url = config['DATABASE_CONFIG']['url'],
                driver = config['DATABASE_CONFIG']['driver'],
                dbtable = config['DATABASE_CONFIG']['dbtable'],
                user = config['DATABASE_CONFIG']['user'],
                password = config['DATABASE_CONFIG']['password'] )\
            .option('partitionColumn', 'encounter_id')\
            .option('numPartitions', 1000)\
            .option('fetchsize', 100000)\
            .option('lowerBound', 1)\
            .option('upperBound', 10000000)\
            .load()