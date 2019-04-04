##
# general oncology treatment dag
##
import sys

sys.path.append('../../scripts/')

import GeneralOncologyTreatment

query = """create table if not exists spark_general_oncology_treatment (
							date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                            person_id int,
							encounter_id int,
							encounter_type int,
							encounter_datetime datetime,
							visit_id int,
							location_id int,
							location_uuid varchar (100),
							#location_name char (100),
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


sc = GeneralOncologyTreatment.create_spark_context('General Oncology Program')
sqlc = GeneralOncologyTreatment.create_sql_context(sc)
flat_obs = GeneralOncologyTreatment.generate_flat_obs_dataframe(sqlc)
GeneralOncologyTreatment.print_sql_schema(flat_obs)
GeneralOncologyTreatment.udfRegistration(sqlc)
general_onc_df = GeneralOncologyTreatment.generate_general_oncology_df(sqlc,flat_obs)
proccesed = GeneralOncologyTreatment.process_general_oncology_df(sqlc, general_onc_df)
# GeneralOncologyTreatment.print_sql_schema(proccesed)
# proccesed.show()

# GeneralOncologyTreatment.create_general_oncology_treatment_table(query)
# GeneralOncologyTreatment.save_processed_data_into_db(proccesed)