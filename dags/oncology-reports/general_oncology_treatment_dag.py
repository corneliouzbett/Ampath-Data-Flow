##
# general oncology treatment dag
##
import sys

sys.path.append('../../scripts/')

import GeneralOncologyTreatment


sc = GeneralOncologyTreatment.create_spark_context('General Oncology Program')
sqlc = GeneralOncologyTreatment.create_sql_context(sc)
flat_obs = GeneralOncologyTreatment.generate_flat_obs_dataframe(sqlc)
GeneralOncologyTreatment.print_sql_schema(flat_obs)
GeneralOncologyTreatment.udfRegistration(sqlc)
general_onc_df = GeneralOncologyTreatment.generate_general_oncology_df(sqlc,flat_obs)
proccesed = GeneralOncologyTreatment.process_general_oncology_df(sqlc, general_onc_df)