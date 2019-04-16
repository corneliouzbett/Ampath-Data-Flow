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
    return re.findall('## !!'+ str(concept) + '=(.+?)!! ##',obs)
  
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
    .option('numPartitions', 350)\
    .option('fetchsize', 5000)\
    .option('lowerBound', 1)\
    .option('upperBound', 7000000)\
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
							end as heart,
                            case
								when obs rlike "!!1125=1118!!" then  1
								when obs rlike "!!1125=1115!!" then  2
                                when obs rlike "!!1125=5105!!" then  3
								when obs rlike "!!1125=5008!!" then  4
                                when obs rlike "!!1125=5009!!" then  5
                                when obs rlike "!!1125=581!!" then  6
                                when obs rlike "!!1125=5103!!" then  7
								else  null
							end as abdomenexam,
                            case
								when obs rlike "!!1126=1118!!" then  1
								when obs rlike "!!1126=1115!!" then  2
                                when obs rlike "!!1126=1116!!" then  3
								when obs rlike "!!1126=2186!!" then  4
                                when obs rlike "!!1126=864!!" then  5
                                when obs rlike "!!1126=6334!!" then  6
                                when obs rlike "!!1126=1447!!" then 7
                                 when obs rlike "!!1126=5993!!" then  8
								when obs rlike "!!1126=8998!!" then  9
                                when obs rlike "!!1126=1489!!" then  10
                                when obs rlike "!!1126=8417!!" then  11
                                when obs rlike "!!1126=8261!!" then  12
								else  null
							end as urogenital,
                            case
								when obs rlike "!!1127=1118!!" then  1
								when obs rlike "!!1127=1115!!" then  2
                                when obs rlike "!!1127=590!!" then  3
								when obs rlike "!!1127=7293!!" then  4
                                when obs rlike "!!1127=134!!" then  5
								else  null
							end as extremities,
                            case
								when obs rlike "!!8420=1118!!" then  1
								when obs rlike "!!8420=1115!!" then  2
                                when obs rlike "!!8420=1116!!" then  3
								else  null
							end as testicular_exam,
                            case
								when obs rlike "!!8420=1118!!" then  1
								when obs rlike "!!8420=1115!!" then  2
                                when obs rlike "!!8420=1116!!" then  3
                                when obs rlike "!!8420=8261!!" then  4
								else  null
							end as nodal_survey,
                            case
								when obs rlike "!!1128=1118!!" then  1
								when obs rlike "!!1128=1115!!" then  2
                                when obs rlike "!!1128=1116!!" then  3
								else  null
							end as musculoskeletal,
                            case
								when obs rlike "!!1120=1107!!" then  1
								when obs rlike "!!1120=1118!!" then  2
                                when obs rlike "!!1120=582!!" then  3
								else  null
							end as skin_exam_findings,
                            case
								when obs rlike "!!8265=6599!!" then   1
								when obs rlike "!!8265=6598!!" then   2
                                when obs rlike "!!8265=1237!!" then   3
                                when obs rlike "!!8265=1236!!" then   4
								when obs rlike "!!8265=1349!!" then   5
                                when obs rlike "!!8265=6601!!" then   6
                                when obs rlike "!!8265=1350!!" then   7
								when obs rlike "!!8265=6600!!" then   8
                                when obs rlike "!!8265=6597!!" then   9
								else   null
							end as body_part,
                            case
								when obs rlike "!!8264=5139!!" then   1
								when obs rlike "!!8264=5141!!" then  2
								else   null
							end as laterality,
                            case
								when obs rlike "!!8270=" then  GetValues(obs,8270) 
								else  null
							end as measure_first_direction,
                            case
								when obs rlike "!!8271=" then  GetValues(obs,8271) 
								else  null
							end as measure_second_direction,
                            case
								when obs rlike "!!1129=1118!!" then   1
								when obs rlike "!!1129=1115!!" then   2
                                when obs rlike "!!1129=599!!" then   3
								when obs rlike "!!1129=497!!" then   4
                                when obs rlike "!!1129=5108!!" then   5
                                when obs rlike "!!1129=6005!!" then   6
								else   null
							end as neurologic,
                            case
								when obs rlike "!!10071=10072!!" then   1
								when obs rlike "!!10071=10073!!" then   2
                                when obs rlike "!!10071=10074!!" then   3
								else   null
							end as bnody_part,
                            case
								when obs rlike "!!6575=1065!!" then   1
								when obs rlike "!!6575=1066!!" then   2
								else   null
							end as patient_on_chemo,
                            case
								when obs rlike "!!1536=1115!!" then   1
								when obs rlike "!!1536=1116!!" then   2
                                when obs rlike "!!1536=1532!!" then   3
								when obs rlike "!!1536=1533!!" then   4
                                when obs rlike "!!1536=1538!!" then   5
                                when obs rlike "!!1536=5622!!" then   6
								else   null
							end as echo,
                            case
								when obs rlike "!!846=1115!!" then  1
								when obs rlike "!!846=1116!!" then  2
								else  null
							end as ct_head,
                             case
								when obs rlike "!!9839=1115!!" then  1
								when obs rlike "!!9839=1116!!" then  2
								else  null
							end as ct_neck,
                            case
								when obs rlike "!!7113=1115!!" then  1
								when obs rlike "!!7113=1116!!" then  2
								else  null
							end as ct_chest,
                            case
								when obs rlike "!!7114=1115!!" then  1
								when obs rlike "!!7114=1116!!" then  2
								else  null
							end as ct_abdomen,
                             case
								when obs rlike "!!9840=1115!!" then  1
								when obs rlike "!!9840=1116!!" then  2
								else  null
							end as ct_spine,
                            case
								when obs rlike "!!9881=1115!!" then  1
								when obs rlike "!!9881=1116!!" then  2
								else  null
							end as mri_head,
                             case
								when obs rlike "!!9882=1115!!" then  1
								when obs rlike "!!9882=1116!!" then  2
								else  null
							end as mri_neck,
                             case
								when obs rlike "!!9883=1115!!" then  1
								when obs rlike "!!9883=1116!!" then  2
								else  null
							end as mri_chest,
                             case
								when obs rlike "!!9884=1115!!" then  1
								when obs rlike "!!9884=1116!!" then  2
								else  null
							end as mri_abdomen,
                            case
								when obs rlike "!!9885=1115!!" then  1
								when obs rlike "!!9885=1116!!" then  2
								else  null
							end as mri_spine,
                            case
								when obs rlike "!!9951=1115!!" then  1
								when obs rlike "!!9951=1116!!" then  2
								else  null
							end as mri_arms,
                            case
								when obs rlike "!!9952=1115!!" then  1
								when obs rlike "!!9952=1116!!" then  2
								else  null
							end as mri_pelvic,
                            case
								when obs rlike "!!9953=1115!!" then   1
								when obs rlike "!!9953=1116!!" then   2
								else   null
							end as mri_legs,
                            case
								when obs rlike "!!7115=1115!!" then  1
								when obs rlike "!!7115=1116!!" then  2
								else  null
							end as ultrasound_renal,
                            case
								when obs rlike "!!852=1115!!" then  1
								when obs rlike "!!852=1116!!" then  2
								else  null
							end as ultrasound_hepatic,
                            case
								when obs rlike "!!6221=1115!!" then  1
								when obs rlike "!!6221=1116!!" then  2
								else  null
							end as obstetric_ultrasound,
                            case
								when obs rlike "!!845=1115!!" then  1
								when obs rlike "!!845=1116!!" then  2
								else  null
							end as abonimal_ultrasound,
                            case
								when obs rlike "!!9596=1115!!" then  1
								when obs rlike "!!9596=1116!!" then  2
								else  null
							end as breast_ultrasound,
                            case
								when obs rlike "!!394=1115!!" then  1
								when obs rlike "!!394=1116!!" then  2
								else  null
							end as x_ray_shoulder,
                            case
								when obs rlike "!!392=1115!!" then  1
								when obs rlike "!!392=1116!!" then  2
								else  null
							end as x_ray_pelvis,
                            case
								when obs rlike "!!101=1115!!" then  1
								when obs rlike "!!101=1116!!" then  2
								else  null
							end as x_ray_abdomen,
                            case
								when obs rlike "!!386=1115!!" then  1
								when obs rlike "!!386=1116!!" then  2
								else  null
							end as x_ray_skull,
                            case
								when obs rlike "!!380=1115!!" then  1
								when obs rlike "!!380=1116!!" then  2
								else  null
							end as x_ray_leg,
                            case
								when obs rlike "!!382=1115!!" then  1
								when obs rlike "!!382=1116!!" then  2
								else  null
							end as x_ray_hand,
                            case
								when obs rlike "!!384=1115!!" then  1
								when obs rlike "!!384=1116!!" then  2
								else  null
							end as x_ray_foot,
                            case
								when obs rlike "!!12=1115!!" then  1
								when obs rlike "!!12=1116!!" then  2
								else  null
							end as x_ray_chest,
                            case
								when obs rlike "!!377=1115!!" then  1
								when obs rlike "!!377=1116!!" then  2
								else  null
							end as x_ray_arm,
                            case
								when obs rlike "!!390=1115!!" then  1
								when obs rlike "!!390=1116!!" then  2
								else  null
							end as x_ray_spine,
                            case
								when obs rlike "!!6504=6505!!" then  1
								when obs rlike "!!6504=6506!!" then  2
                                when obs rlike "!!6504=6507!!" then  3
								when obs rlike "!!6504=6508!!" then  4
                                when obs rlike "!!6504=6902!!" then  5
								when obs rlike "!!6504=7189!!" then  6
								else  null
							end as method_of_diagnosis,
                            case
								when obs rlike "!!6509=7190!!" then  1
								when obs rlike "!!6509=6510!!" then  2
                                when obs rlike "!!6509=6511!!" then  3
								when obs rlike "!!6509=6512!!" then  4
                                when obs rlike "!!6509=6513!!" then  5
								when obs rlike "!!6509=10075!!" then  6
                                when obs rlike "!!6509=10076!!" then  7
								else  null
							end as diagnosis_on_biopsy,
                            case
								when obs rlike "!!6605=1065!!" then  1
								when obs rlike "!!6605=1066!!" then  2
								else  null
							end as biopsy_consistent_clinic_susp,
                            case
								when obs rlike "!!7191=" then GetValues(obs,7191) 
								else null
							end as diagnosis_results,
                            case
								when obs rlike "!!7176=6485!!" then   1
								when obs rlike "!!7176=6514!!" then   2
                                when obs rlike "!!7176=6520!!" then   3
								when obs rlike "!!7176=6528!!" then   4
                                when obs rlike "!!7176=6536!!" then   5
								when obs rlike "!!7176=6551!!" then   6
                                when obs rlike "!!7176=6540!!" then   7
                                when obs rlike "!!7176=6544!!" then   8
                                when obs rlike "!!7176=216!!" then   9
                                when obs rlike "!!7176=9846!!" then   10
                                when obs rlike "!!7176=5622!!" then   11
								else   null
							end as cancer_type,
                            case
								when obs rlike "!!1915=" then GetValues(obs,1915) 
								else  null
							end as other_cancer,
                            case
								when obs rlike "!!9843=507!!" then   1
								when obs rlike "!!9843=6486!!" then   2
                                when obs rlike "!!9843=6487!!" then   3
								when obs rlike "!!9843=6488!!" then   4
                                when obs rlike "!!9843=6489!!" then   5
								when obs rlike "!!9843=6490!!" then   6
								else   null
							end as type_of_sarcoma,
                            case
								when obs rlike "!!6514=6515!!" then  1
								when obs rlike "!!6514=6516!!" then  2
                                when obs rlike "!!6514=6517!!" then  3
								when obs rlike "!!6514=6518!!" then  4
                                when obs rlike "!!6514=6519!!" then  5
								when obs rlike "!!6514=5622!!" then  6
								else  null
							end as type_of_gu,
                            case
								when obs rlike "!!6520=6521!!" then  1
								when obs rlike "!!6520=6522!!" then  2
                                when obs rlike "!!6520=6523!!" then  3
								when obs rlike "!!6520=6524!!" then  4
                                when obs rlike "!!6520=6525!!" then  5
								when obs rlike "!!6520=6526!!" then  6
                                when obs rlike "!!6520=6527!!" then  7
                                when obs rlike "!!6520=6568!!" then  8
								when obs rlike "!!6520=5622!!" then  9
								else  null
							end as type_of_gi_cancer,
                            case
								when obs rlike "!!6528=6529!!" then  1
								when obs rlike "!!6528=6530!!" then  2
                                when obs rlike "!!6528=6531!!" then  3
								when obs rlike "!!6528=6532!!" then  4
                                when obs rlike "!!6528=6533!!" then  5
								when obs rlike "!!6528=6534!!" then  6
                                when obs rlike "!!6528=5622!!" then  7
								else  null
							end as head_and_neck_cancer,
                            case
								when obs rlike "!!6536=6537!!" then  1
								when obs rlike "!!6536=6538!!" then  2
                                when obs rlike "!!6536=6539!!" then  3
								when obs rlike "!!6536=5622!!" then  4
								else  null
							end as gynecologic_cancer,
                            case
								when obs rlike "!!6551=6553!!" then  1
								when obs rlike "!!6551=6552!!" then  2
                                when obs rlike "!!6551=8423!!" then  3
								when obs rlike "!!6551=5622!!" then  4
								else  null
							end as lympoma_cancer,
                            case
								when obs rlike "!!6540=6541!!" then  1
								when obs rlike "!!6540=6542!!" then  2
                                when obs rlike "!!6540=6543!!" then  3
								when obs rlike "!!6540=5622!!" then  4
								else  null
							end as skin_cancer,
                            case
								when obs rlike "!!9841=6545!!" then  1
								when obs rlike "!!9841=9842!!" then  2
                                when obs rlike "!!9841=5622!!" then  3
								else  null
							end as breast_cancer,
                            case
								when obs rlike "!!9846=8424!!" then  1
								when obs rlike "!!9846=8425!!" then  2
                                when obs rlike "!!9846=9845!!" then  3
                                when obs rlike "!!9846=5622!!" then  4
								else  null
							end as other_solid_cancer,
                            case
								when obs rlike "!!9844=6547!!" then  1
								when obs rlike "!!9844=6548!!" then  2
                                when obs rlike "!!9844=6549!!" then  3
                                when obs rlike "!!9844=6550!!" then  4
                                when obs rlike "!!9844=5622!!" then  5
								else  null
							end as type_of_leukemeia,
                            case
								when obs rlike "!!9847=1226!!" then  1
								when obs rlike "!!9847=6556!!" then  2
                                when obs rlike "!!9847=9870!!" then  3
                                when obs rlike "!!9847=2!!" then  4
                                when obs rlike "!!9847=6557!!" then  5
                                when obs rlike "!!9847=5622!!" then  6
								else  null
							end as non_cancer,
                            case
								when obs rlike "!!9728=" then  GetValues(obs,9728) 
								else  null
							end as diagnosis_date,
							case
								when obs rlike "!!9848=9849!!" then   1
								when obs rlike "!!9848=9850!!" then   2
								else   null
							end as new_or_recurrence_cancer,
                            case
								when obs rlike "!!6582=1067!!" then  1
								when obs rlike "!!6582=6566!!" then  2
                                when obs rlike "!!6582=10206!!" then  3
                                when obs rlike "!!6582=1175!!" then  4
								else  null
							end as cancer_stage,
                            case
								when obs rlike "!!9868=9851!!" then  1
								when obs rlike "!!9868=9852!!" then  2
                                when obs rlike "!!9868=9853!!" then  3
                                when obs rlike "!!9868=9854!!" then  4
                                when obs rlike "!!9868=9855!!" then  5
								when obs rlike "!!9868=9856!!" then  6
                                when obs rlike "!!9868=9857!!" then  7
                                when obs rlike "!!9868=9858!!" then  8
                                when obs rlike "!!9868=9859!!" then  9
								when obs rlike "!!9868=9860!!" then  10
                                when obs rlike "!!9868=9861!!" then  11
                                when obs rlike "!!9868=9862!!" then  12
                                when obs rlike "!!9868=9863!!" then  13
								when obs rlike "!!9868=9864!!" then  14
                                when obs rlike "!!9868=9865!!" then  15
                                when obs rlike "!!9868=9866!!" then  16
                                when obs rlike "!!9868=9867!!" then  17
								else  null
							end as overall_cancer_staging,
							case
								when obs rlike "!!1271=1019!!" then  1
								when obs rlike "!!1271=790!!" then  2
                                when obs rlike "!!1271=953!!" then  3
                                when obs rlike "!!1271=6898!!" then  4
                                when obs rlike "!!1271=9009!!" then  5
								when obs rlike "!!1271=1327!!" then  6
                                when obs rlike "!!1271=10205!!" then  7
                                when obs rlike "!!1271=8595!!" then  8
                                when obs rlike "!!1271=8596!!" then  9
								when obs rlike "!!1271=10125!!" then  10
                                when obs rlike "!!1271=10123!!" then  11
                                when obs rlike "!!1271=846!!" then  12
                                when obs rlike "!!1271=9839!!" then  13
								when obs rlike "!!1271=7113!!" then  14
                                when obs rlike "!!1271=7114!!" then  15
                                when obs rlike "!!1271=9840!!" then  16
                                when obs rlike "!!1271=1536!!" then  17
                                when obs rlike "!!1271=9881!!" then  18
								when obs rlike "!!1271=9882!!" then  19
                                when obs rlike "!!1271=9883!!" then  20
                                when obs rlike "!!1271=9951!!" then  21
                                when obs rlike "!!1271=9884!!" then  22
								when obs rlike "!!1271=9952!!" then  23
                                when obs rlike "!!1271=9885!!" then  24
                                when obs rlike "!!1271=9953!!" then  25
                                when obs rlike "!!1271=845!!" then  26
								when obs rlike "!!1271=9596!!" then  27
                                when obs rlike "!!1271=7115!!" then  28
                                when obs rlike "!!1271=852!!" then  29
                                when obs rlike "!!1271=394!!" then  30
								when obs rlike "!!1271=392!!" then  31
                                when obs rlike "!!1271=101!!" then  32
                                when obs rlike "!!1271=386!!" then  33
                                when obs rlike "!!1271=380!!" then  34
                                 when obs rlike "!!1271=882!!" then  35
								when obs rlike "!!1271=384!!" then  36
                                when obs rlike "!!1271=12!!" then  37
                                when obs rlike "!!1271=377!!" then  38
                                when obs rlike "!!1271=390!!" then  39
								else  null
							end as test_ordered,
                            case
								when obs rlike "!!8190=" then  GetValues(obs,8190) 
								else  null
							end as other_radiology,
                            case
								when obs rlike "!!9538=" then  GetValues(obs,9538) 
								else  null
							end as other_laboratory,
                            case
								when obs rlike "!!8723=7465!!" then  1
								when obs rlike "!!8723=8427!!" then  2
                                when obs rlike "!!8723=6575!!" then  3
                                when obs rlike "!!8723=9626!!" then  4
                                when obs rlike "!!8723=10038!!" then  5
                                when obs rlike "!!8723=10232!!" then  6
                                when obs rlike "!!8723=5622!!" then  7
								else  null
							end as treatment_plan,
                            case
								when obs rlike "!!10039=" then  GetValues(obs,10039) 
								else  null
							end as other_treatment_plan,
                            case
								when obs rlike "!!6419=1065!!" then  1
								when obs rlike "!!6419=1066!!" then  2
								else  null
							end as hospitalization,
                            case
								when obs rlike "!!9916=1349!!" then  1
								when obs rlike "!!9916=8193!!" then  2
                                when obs rlike "!!9916=9226!!" then  3
                                when obs rlike "!!9916=9227!!" then  4
                                when obs rlike "!!9916=9228!!" then  5
                                when obs rlike "!!9916=9229!!" then  6
                                when obs rlike "!!9916=9230!!" then  7
                                when obs rlike "!!9916=9231!!" then  8
                                when obs rlike "!!9916=5622!!" then  9
								else  null
							end as radiation_location,
                             case
								when obs rlike "!!8725=8428!!" then  1
								when obs rlike "!!8725=8724!!" then  2
                                when obs rlike "!!8725=8727!!" then  3
                                when obs rlike "!!8725=10233!!" then  4
								else  null
							end as surgery_reason,
                            case
								when obs rlike "!!10230=" then   GetValues(obs,10230) 
								else   null
							end as surgical_procedure,
                            case
								when obs rlike "!!9918=8513!!" then  1
								when obs rlike "!!9918=8514!!" then  2
                                when obs rlike "!!9918=8519!!" then  3
                                when obs rlike "!!9918=8515!!" then  4
                                when obs rlike "!!9918=8518!!" then  5
								when obs rlike "!!9918=7217!!" then  6
                                when obs rlike "!!9918=10140!!" then  7
                                when obs rlike "!!9918=5622!!" then  8
								else  null
							end as hormonal_drug,
                            case
								when obs rlike "!!10234=8489!!" then  1
								when obs rlike "!!10234=8498!!" then  2
                                when obs rlike "!!10234=8501!!" then  3
                                when obs rlike "!!10234=5622!!" then  4
								else  null
							end as targeted_therapies,
                            case
								when obs rlike "!!1896=1891!!" then  1
								when obs rlike "!!1896=1099!!" then  2
                                when obs rlike "!!1896=7682!!" then  3
                                when obs rlike "!!1896=9933!!" then  4
                                when obs rlike "!!1896=1098!!" then  5
                                when obs rlike "!!1896=7772!!" then  6
								else  null
							end as targeted_therapies_frequency,
                             case
								when obs rlike "!!10235=" then  GetValues(obs,10235) 
								else  null
							end as targeted_therapy_start_date,
                            case
								when obs rlike "!!10236=" then  GetValues(obs,10236) 
								else  null
							end as targeted_therapy_stop_date,
                            case
								when obs rlike "!!9869=1256!!" then  1
								when obs rlike "!!9869=1259!!" then  2
                                when obs rlike "!!9869=1260!!" then  3
                                when obs rlike "!!9869=1257!!" then  4
                                when obs rlike "!!9869=6576!!" then  5
								else  null
							end as chemotherapy_plan,
                            case
								when obs rlike "!!9917=1065!!" then  1
								when obs rlike "!!9917=1066!!" then  2
								else  null
							end as treatment_on_clinical_trial,
                            case
								when obs rlike "!!1190=" then  GetValues(obs,1190) 
								else  null
							end as chemo_start_date,
                            case
								when obs rlike "!!2206=9218!!" then  1
								when obs rlike "!!2206=8428!!" then  2
                                when obs rlike "!!2206=8724!!" then  3
                                when obs rlike "!!2206=9219!!" then  4
								else  null
							end as treatment_intent,
                            case
								when obs rlike "!!9946=" then  GetValues(obs,9946) 
								else  null
							end as chemo_regimen,
                            case
								when obs rlike "!!6643=" then  GetValues(obs,6643) 
								else  null
							end as chemo_cycle,
                            case
								when obs rlike "!!6644=" then  GetValues(obs,6644) 
								else  null
							end as total_planned_chemo_cycle,
                            case
								when obs rlike "!!9918=8478!!" then 1
								when obs rlike "!!9918=7217!!" then 2
                                when obs rlike "!!9918=7203!!" then 3
                                when obs rlike "!!9918=7215!!" then 4
                                when obs rlike "!!9918=7223!!" then 5
								when obs rlike "!!9918=7198!!" then 6
                                when obs rlike "!!9918=491!!" then 7
                                when obs rlike "!!9918=7199!!" then 8
                                when obs rlike "!!9918=7218!!" then 9
								when obs rlike "!!9918=7205!!" then 10
                                when obs rlike "!!9918=7212!!" then 11
                                when obs rlike "!!9918=8513!!" then 12
                                when obs rlike "!!9918=7202!!" then 13
								when obs rlike "!!9918=7209!!" then 14
                                when obs rlike "!!9918=7197!!" then 15
                                when obs rlike "!!9918=8506!!" then 16
                                when obs rlike "!!9918=7210!!" then 17
                                when obs rlike "!!9918=8499!!" then 18
								when obs rlike "!!9918=8492!!" then 19
                                when obs rlike "!!9918=8522!!" then 20
                                when obs rlike "!!9918=8510!!" then 21
                                when obs rlike "!!9918=8511!!" then 22
								when obs rlike "!!9918=9919!!" then 23
                                when obs rlike "!!9918=8518!!" then 24
                                
                                when obs rlike "!!9918=8516!!" then 25
                                when obs rlike "!!9918=8481!!" then 26
								when obs rlike "!!9918=7207!!" then 27
                                when obs rlike "!!9918=7204!!" then 28
                                when obs rlike "!!9918=7201!!" then 29
                                when obs rlike "!!9918=8519!!" then 30
								when obs rlike "!!9918=8507!!" then 31
                                when obs rlike "!!9918=8479!!" then 32
                                when obs rlike "!!9918=7213!!" then 33
                                when obs rlike "!!9918=10139!!" then 34
                                 when obs rlike "!!9918=10140!!" then 35
								when obs rlike "!!9918=5622!!" then 36
								else null
							end as chemo_drug,
                            case
								when obs rlike "!!1899=" then  GetValues(obs,1899) 
								else  null
							end as dosage_in_milligrams,
                            case
								when obs rlike "!!7463=7458!!" then  1
								when obs rlike "!!7463=10078!!" then  2
                                when obs rlike "!!7463=10079!!" then  3
                                when obs rlike "!!7463=7597!!" then  4
                                when obs rlike "!!7463=7581!!" then  5
                                when obs rlike "!!7463=7609!!" then  6
                                when obs rlike "!!7463=7616!!" then  7
                                when obs rlike "!!7463=7447!!" then  8
								else  null
							end as drug_route,
                            case
								when obs rlike "!!1895=" then  GetValues(obs,1895) 
								else  null
							end as other_drugs,
                            case
								when obs rlike "!!1779=" then  GetValues(obs,1779) 
								else  null
							end as other_medication,
                            case
								when obs rlike "!!2206=9220!!" then  1
								when obs rlike "!!2206=8428!!" then  2
								else  null
							end as purpose,
                             case
								when obs rlike "!!1272=1107!!" then  1
								when obs rlike "!!1272=8724!!" then  2
                                when obs rlike "!!1272=5484!!" then  3
                                when obs rlike "!!1272=7054!!" then  4
                                when obs rlike "!!1272=6419!!" then  5
                                when obs rlike "!!1272=5490!!" then  6
                                when obs rlike "!!1272=5486!!" then  7
                                when obs rlike "!!1272=5483!!" then  8
                                when obs rlike "!!1272=5622!!" then  9
								else  null
							end as referral_ordered,
                            case
								when obs rlike "!!5096=" then  GetValues(obs,5096) 
								else  null
							end as return_to_clinic_date
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
    proccesed_df.coalesce(10).write\
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