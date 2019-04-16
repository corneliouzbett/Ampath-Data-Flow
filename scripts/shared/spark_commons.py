from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import lit
from pyspark.sql.types import StringType


class commons:

    def __init__(self):
        pass

    # create spark context
    def create_spark_context(name):
        sc_conf = SparkConf()
        sc_conf.setAppName(name)
        sc_conf.setMaster('local[*]')
        sc_conf.set('spark.executor.memory', '2g')
        sc_conf.set('spark.executor.cores', '4')
        sc_conf.set('spark.cores.max', '40')
        sc_conf.set('spark.logConf', True)

        print sc_conf.getAll()
        sc = None
        try:
            sc.stop()
            sc = SparkContext(conf=sc_conf)
        except:
            sc = SparkContext(conf=sc_conf)

        return sc 

    def create_sql_context(spark_context):
        print('=========== creating spark sql context =============')
        return SQLContext(spark_context)

    def register_udf(sql_context, function, udf_name):
        sql_context.registerFunction(udf_name,function)

    
    def print_sql_schema(dataframe):
        dataframe.printSchema()