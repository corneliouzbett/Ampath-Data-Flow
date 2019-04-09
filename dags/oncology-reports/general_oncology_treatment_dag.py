##
# general oncology treatment dag
##
import sys
import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

sys.path.append('../../scripts/')

import GeneralOncologyTreatment

default_args = {
    'owner': 'corneliouzbett',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    'email': ['kibett@ampath.or.ke'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'adhoc':False,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'trigger_rule': u'all_success'
}

dag = DAG(
    'General Oncology treatment dag',
    default_args=default_args,
    description='It generates reports for general oncology treatment program',
    schedule_interval=timedelta(days=1),
)

query = """create table if not exists spark_general_oncology_treatment (
							date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                            person_id int,
							encounter_id int,
							encounter_type int,
							encounter_datetime datetime,
							visit_id int,
							location_id int,
							location_uuid varchar (100),
							gender char (100),
							age int,
                            cur_visit_type INT,
							ecog_performance INT,
							general_exam INT,
							heent INT,
							breast_findings INT,
							breast_finding_location INT,
							breast_findingquadrant INT,
							chest INT,
							heart INT,
							diagnosis_results varchar(500)
						);"""

t1 = PythonOperator(
    task_id ='spark session',
    python_callable ='GeneralOncologyTreatment.create_spark_context',
	op_kwargs = {'name': "General Oncology treatment"},
    dag=dag,
)

t1.doc_md = """\
#### Task Documentation
creating spark context
"""

dag.doc_md = __doc__

t2 = PythonOperator(
    task_id ='sql context',
    python_callable ='GeneralOncologyTreatment.create_sql_context',
    dag=dag,
)

# sc = GeneralOncologyTreatment.create_spark_context('General Oncology Program')
# sqlc = GeneralOncologyTreatment.create_sql_context(sc)
# flat_obs = GeneralOncologyTreatment.generate_flat_obs_dataframe(sqlc)
# GeneralOncologyTreatment.print_sql_schema(flat_obs)
# GeneralOncologyTreatment.udfRegistration(sqlc)
# eneral_onc_df = GeneralOncologyTreatment.generate_general_oncology_df(sqlc,flat_obs)
# proccesed = GeneralOncologyTreatment.process_general_oncology_df(sqlc, general_onc_df)
# GeneralOncologyTreatment.print_sql_schema(proccesed)
# proccesed.show()

# GeneralOncologyTreatment.create_general_oncology_treatment_table(query)
# GeneralOncologyTreatment.save_processed_data_into_db(proccesed)

t1 >> t2