##
# general oncology treatment dag
##

from scripts.GeneralOncologyTreatment import GeneralOncologyTreatment


sc = GeneralOncologyTreatment.create_spark_context('General Oncology Program')
sqlc = GeneralOncologyTreatment.create_sql_context(sc)
flat_obs = GeneralOncologyTreatment.generate_flat_obs_dataframe(sqlc)
GeneralOncologyTreatment.print_sql_schema(flat_obs)