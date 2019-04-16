from shared.spark_commons import commons
from shared.amrs_obs import AmrsObs

class GeneralOncologyTreatment(AmrsObs):
    
    def __init__(self, sql_context):
        AmrsObs.__init__(self, sql_context)


    def generate_general_oncology_treatment_df(self):
        super(GeneralOncologyTreatment, self).generate_obs_dataframe()\
            .createGlobalTempView("general_oncology_treatment_temp")
        gen_onc_tr_df = self.sql_context.sql(
            """
            SELECT * FROM global_temp.general_oncology_treatment_temp where encounter_id in (42, 43)
            """
        )
        gen_onc_tr_df.printSchema()
        return gen_onc_tr_df


common = commons('general_oncology_treatment')
context = common.create_context()

general_oncology_treatment = LungCancerTreatment(context)
general_oncology_treatment.generate_lung_cancer_treatment_df()
