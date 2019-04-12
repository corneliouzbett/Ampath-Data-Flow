"""
### General oncology treatment
 it extract general oncology data from flat obs

"""
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
    'start_date': airflow.utils.dates.days_ago(1),
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

t1 = = BashOperator(
    task_id='sleep',
    depends_on_past=False,
    bash_command='spark-submit --jars ~/Downloads/mysql-connector-java-5.1.45-bin.jar GeneralOncologyTreatment.py',
    dag=dag,
)

t1.doc_md = """\
#### General oncology Documentation
creating spark context
"""

dag.doc_md = __doc__

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

t1