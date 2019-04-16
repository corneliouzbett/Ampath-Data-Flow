from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf

import os
import sys

SUBMIT_ARGS = "--jars ~/Downloads/mysql-connector-java-5.1.45-bin.jar pyspark-shell"
os.environ["PYSPARK_SUBMIT_ARGS"] = SUBMIT_ARGS


class commons:

    def __init__(self, name):
        self.name = name

    # create spark context
    def create_context(self):
        sc_conf = SparkConf()
        sc_conf.setAppName(self.name)
        sc_conf.setMaster('local[*]')
        sc_conf.set('spark.executor.memory', '2g')
        sc_conf.set('spark.executor.cores', '4')
        sc_conf.set('spark.cores.max', '40')
        sc_conf.set('spark.logConf', True)
        sc_conf.set('spark.debug.maxToStringFields', '100')

        print sc_conf.getAll()
        sc = None
        try:
            sc.stop()
            sc = SparkContext(conf=sc_conf)
        except:
            sc = SparkContext(conf=sc_conf)

        return SQLContext(sc)

    def create_sql_context(spark_context):
        print('=========== creating spark sql context =============')
        return SQLContext(spark_context)

    def register_udf(sql_context, function, udf_name):
        sql_context.registerFunction(udf_name,function)

    
    def print_sql_schema(dataframe):
        dataframe.printSchema()
