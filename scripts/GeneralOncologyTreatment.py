from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import lit

import os
import re

SUBMIT_ARGS = "--jars ~/Downloads/mysql-connector-java-5.1.45-bin.jar pyspark-shell"
os.environ["PYSPARK_SUBMIT_ARGS"] = SUBMIT_ARGS

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

# create spark sql context
def create_sql_context(spark_context):
    return SQLContext(spark_context)

def generate_flat_obs_dataframe(sqlContext):

    return sqlContext.read\
    .format('jdbc')\
    .options(
        url = 'jdbc:mysql://10.50.80.45:3309/etl',
        driver = 'com.mysql.jdbc.Driver',
        dbtable = 'flat_obs',
        user = 'fmaiko',
        password = 'Ampath123' )\
    .option('partitionColumn', 'encounter_id')\
    .option('numPartitions', 500)\
    .option('fetchsize', 5000)\
    .option('lowerBound', 1)\
    .option('upperBound', 11521481695656862L)\
    .load()

def print_sql_schema(df):
    df.printSchema()

def generate_general_oncology_df(flat_obs, sqlContext):
    flat_obs.createGlobalTempView("flat_obs_temp")
    general_oncology_df = sqlContext.sql(
        """
            SELECT * FROM global_temp.flat_obs_temp where encounter_type in (38, 39)
        """
    )
    general_oncology_df.show()



sc = create_spark_context('General Oncology Program')
sqlc = create_sql_context(sc)
flat_obs = generate_flat_obs_dataframe(sqlc)
print_sql_schema(flat_obs)
generate_general_oncology_df(flat_obs,sqlc )