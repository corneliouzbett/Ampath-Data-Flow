from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import lit
from pyspark.sql.types import StringType

import os
import re
import sys
import json

#from shared.MysqlDb import MysqlHelper
#from udf import functions as udf

#with open('../../config/config.json') as f:
with open('/home/corneliouzbett/Documents/projects/Ampath-Data-Flow/config/config.json') as f:
    config = json.load(f)

SUBMIT_ARGS = "--jars ~/Downloads/mysql-connector-java-5.1.45-bin.jar pyspark-shell"
os.environ["PYSPARK_SUBMIT_ARGS"] = SUBMIT_ARGS

def searchObsValue(obs, concept):
    try:
        found = re.search('## !!'+concept+'=(.+?)!! ##',obs)
    except AttributeError:
        found = 'null'
    return found
  
def udfRegistration(sqlContext):
    sqlContext.registerFunction("GetValues",searchObsValue)

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
    print('creating sql context')
    return SQLContext(spark_context)

def generate_flat_obs_dataframe(sqlContext):
    print('========================== create flat obs data frame ========================')
    return sqlContext.read\
    .format('jdbc')\
    .options(
        url = config['DATABASE_CONFIG']['url'],
        driver = config['DATABASE_CONFIG']['driver'],
        dbtable = config['DATABASE_CONFIG']['dbtable'],
        user = config['DATABASE_CONFIG']['user'],
        password = config['DATABASE_CONFIG']['password'] )\
    .option('partitionColumn', 'encounter_id')\
    .option('numPartitions', 500)\
    .option('fetchsize', 5000)\
    .option('lowerBound', 1)\
    .option('upperBound', 11521481695656862L)\
    .load()

def print_sql_schema(df):
    df.printSchema()

def generate_general_oncology_df(sqlContext, flat_obs):
    print('========================== create flat obs temporary view ========================')
    flat_obs.createGlobalTempView("flat_obs_temp")
    gen_onc_df = sqlContext.sql(
        """
            SELECT * FROM global_temp.flat_obs_temp where encounter_type in (38, 39)
        """
    )
    gen_onc_df.printSchema()
    return gen_onc_df


def process_general_oncology_df(sqlContext, gen_onc_df):
    print('========================== create general oncology temporary view ========================')
    gen_onc_df.createGlobalTempView("general_onc_temp")
    proccesed_gen_onc_df = sqlContext.sql("""
    select person_id, encounter_id, encounter_type,location_id,obs,
                            case
                                when obs rlike "!!1834=1068!!" then 1
                                when obs rlike "!!1834=7850!!" then 2
                                when obs rlike "!!1834=1246!!" then 3
                                when obs rlike "!!1834=2345!!" then 4
                                when obs rlike "!!1834=10037!!" then 5
                                else null
                            end as cur_visit_type,
                            case
								when obs  rlike "!!6584=1115!!" then 1
								when obs  rlike "!!6584=6585!!" then 2
                                when obs  rlike "!!6584=6586!!" then 3
                                when obs  rlike "!!6584=6587!!" then 4
                                when obs  rlike "!!6584=6588!!" then 5
								else null
							end as ecog_performance,
                             case
								when obs  rlike "!!1119=1115!!" then 1
								when obs  rlike "!!1119=5201!!" then 2
                                when obs  rlike "!!1119=5245!!" then 3
                                when obs  rlike "!!1119=215!!" then 4
                                when obs  rlike "!!1119=589!!" then 5
								else null
							end as general_exam,
                            case
								when obs  rlike "!!1122=1118!!" then 1
								when obs  rlike "!!1122=1115!!" then 2
                                when obs  rlike "!!1122=5192!!" then 3
                                when obs  rlike "!!1122=516!!" then 4
                                when obs  rlike "!!1122=513!!" then 5
                                when obs  rlike "!!1122=5170!!" then 6
                                when obs  rlike "!!1122=5334!!" then 7
                                when obs  rlike "!!1122=6672!!" then 8
								else null
							end as HEENT,
                            case
								when obs  rlike "!!6251=1115!!" then 1
								when obs  rlike "!!6251=9689!!" then 2
                                when obs  rlike "!!6251=9690!!" then 3
                                when obs  rlike "!!6251=9687!!" then 4
                                when obs  rlike "!!6251=9688!!" then 5
                                when obs  rlike "!!6251=5313!!" then 6
                                when obs  rlike "!!6251=6493!!" then 7
                                when obs  rlike "!!6251=6250!!" then 8
								else null
							end as breast_findings,
                            case
								when obs  rlike "!!9696=5141!!" then 1
								when obs  rlike "!!9696=5139!!" then 2
								else null
							end as breast_finding_location,
                            
                            case
								when obs  rlike "!!1123=1118!!" then 1
								when obs  rlike "!!1123=1115!!" then 2
                                when obs  rlike "!!1123=5138!!" then 3
								when obs  rlike "!!1123=5115!!" then 4
                                when obs  rlike "!!1123=5116!!" then 5
                                when obs  rlike "!!1123=5181!!" then 6
                                when obs  rlike "!!1123=5127!!" then 7
								else null
							end as chest,
                             case
								when obs  rlike "!!1124=1118!!" then 1
								when obs  rlike "!!1124=1115!!" then 2
                                when obs  rlike "!!1124=1117!!" then 3
								when obs  rlike "!!1124=550!!" then 4
                                when obs  rlike "!!1124=5176!!" then 5
                                when obs  rlike "!!1124=5162!!" then 6
                                when obs  rlike "!!1124=5164!!" then  7
								else null
							end as heart
      from global_temp.general_onc_temp
    """)
    proccesed_gen_onc_df.drop('obs')
    proccesed_gen_onc_df.schema
    # proccesed_gen_onc_df.show()
    return proccesed_gen_onc_df

#def create_general_oncology_treatment_table(query):
#    mysql_helper = MysqlHelper()
#    mysql_helper.execute_query(query)


def save_processed_data_into_db(proccesed_df):
    proccesed_df.drop(proccesed_df.obs)
    proccesed_df.write\
    .format('jdbc')\
    .options(
        url = config['DATABASE_CONFIG']['url'],
        driver = config['DATABASE_CONFIG']['driver'],
        dbtable = config['GENERAL_ONCOLOGY']['dbtable'],
        user = config['DATABASE_CONFIG']['user'],
        password = config['DATABASE_CONFIG']['password'] )\
    .mode('append')\
    .save()

# spark context
# spark sql context
sc = create_spark_context('General Oncology Program')
sqlc = create_sql_context(sc)
udfRegistration(sqlc)


flat_obs = generate_flat_obs_dataframe(sqlc)
print_sql_schema(flat_obs)

gen_onc = generate_general_oncology_df(sqlc, flat_obs)
processed_data = process_general_oncology_df(sqlc, gen_onc)

save_processed_data_into_db(processed_data)

def insert_gen_onc_data_into_db(gen_onc_df):
    gen_onc_df.createGlobalTempView()