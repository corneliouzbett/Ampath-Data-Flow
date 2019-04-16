from base_amrs import BaseAmrs


class AmrsObs(BaseAmrs):
    
    def __init__(self, sql_context):
        BaseAmrs.__init__(self, sql_context)

    def generate_obs_dataframe(self):
        obs = super(AmrsObs, self).create_dataframe("amrs_obs").alias('obs')
        enc = super(AmrsObs, self).create_dataframe('encounters').alias('enc')

        f_obs = obs.join(enc, obs.encounter_id == enc.encounter_id, how = 'left')

        return f_obs.select("obs.encounter_id", "obs.location_id", "obs_id", "person_id", "patient_id", "concept_id", "obs_datetime",
        "value_group_id", "value_coded", "value_coded_name_id","value_drug", "value_datetime", "value_numeric",
        "value_modifier", "value_text", "encounter_datetime","visit_id")

