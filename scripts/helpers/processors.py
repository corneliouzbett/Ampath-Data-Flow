
class processors:

    def __init__(self, context, dataframe):
        self.dataframe = dataframe
        self.context = context


    def process_general_onc_treatment(self):
        self.dataframe.createGlobalTempView("general_onc_temp")
        return self.context.sql(
            """
            select person_id, encounter_id
          from global_temp.general_onc_temp
        """
        )

    
    def process_lung_cancer_treatment(self):
        self.dataframe.createGlobalTempView("lung_cancer_treatment_temp")
        return self.context.sql(
            """
            """
        )