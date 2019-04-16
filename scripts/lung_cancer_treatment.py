from shared.spark_commons import commons
from shared.amrs_obs import AmrsObs

class LungCancerTreatment(AmrsObs):
    
    def __init__(self, sql_context):
        AmrsObs.__init__(self, sql_context)


    def generate_lung_cancer_treatment_df(self):
        print('===================== creating lung cancer treatment dataframe from amrs obs =================================')
        super(LungCancerTreatment, self).generate_obs_dataframe()\
            .createGlobalTempView("lung_cancer_treatment_temp")
        
        lung_cnr_tr_df = self.sql_context.sql(
            """
            SELECT * FROM global_temp.lung_cancer_treatment_temp
            """
        )
        lung_cnr_tr_df.printSchema()
        return lung_cnr_tr_df


common = commons('lung_cancer_treatment')
context = common.create_context()

lung_cancer_treatment = LungCancerTreatment(context)
lung_cancer_treatment.generate_lung_cancer_treatment_df()
