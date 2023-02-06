from healthrex_ml.extractors.starr_extractors import add_create_or_append_logic
class PatientProblemGroupExtractor():
    """
    Defines logic to extract diagnoses on the patient's problem list
    """

    def __init__(self, cohort_table_id, feature_table_id,
                 project_id='som-nero-phi-jonc101', dataset='shc_core_2021'):
        """
        Args:
            cohort_table: name of cohort table -- used to join to features
            project_id: name of project you are extracting data from
            dataset: name of dataset you are extracting data from
        """
        self.cohort_table_id = cohort_table_id
        self.feature_table_id = feature_table_id
        self.client = bigquery.Client()
        self.project_id = project_id
        self.dataset = dataset

    def __call__(self):
        """
        Executes queries and returns all 
        """
        query = f"""
        SELECT
            labels.observation_id,
            labels.index_time,
            '{self.__class__.__name__}' as feature_type,
            CAST(dx.start_date_utc as TIMESTAMP) as feature_time,
            GENERATE_UUID() as feature_id,
            ccsr.CCSR_CATEGORY_1 as feature,
            1 value
        FROM
            ({self.cohort_table_id}
            labels
        LEFT JOIN
            {self.project_id}.{self.dataset}.diagnosis dx
        ON
            labels.anon_id = dx.anon_id)
        LEFT JOIN 
            mining-clinical-decisions.mapdata.ahrq_ccsr_diagnosis ccsr
        ON
            dx.icd10 = ccsr.icd10
        WHERE 
            CAST(dx.start_date_utc as TIMESTAMP) < labels.index_time
        AND
            source = 2 --problem list only
        """
        query = add_create_or_append_logic(query, self.feature_table_id)
        query_job = self.client.query(query)
        query_job.result()
        
  
class MedicationGroupExtractor():
    """
    Defines logic to extract medication orders
    """

    def __init__(self, cohort_table_id, feature_table_id,
                 look_back_days=28, project_id='som-nero-phi-jonc101',
                 dataset='shc_core_2021'):
        """
        Args:
            cohort_table: name of cohort table -- used to join to features
            project_id: name of project you are extracting data from
            dataset: name of dataset you are extracting data from
        """
        self.cohort_table_id = cohort_table_id
        self.look_back_days = look_back_days
        self.project_id = project_id
        self.dataset = dataset
        self.feature_table_id = feature_table_id
        self.client = bigquery.Client()

    def __call__(self):
        """
        Executes queries and returns all 
        """
        query = f"""
        SELECT DISTINCT
            labels.observation_id,
            labels.index_time,
            '{self.__class__.__name__}' as feature_type,
            meds.order_inst_utc as feature_time,
            CAST(meds.order_med_id_coded as STRING) as feature_id,
            meds.thera_class_abbr as feature,
            1 as value
        FROM
            {self.cohort_table_id}
            labels
        LEFT JOIN
            {self.project_id}.{self.dataset}.order_med meds
        ON
            labels.anon_id = meds.anon_id
        WHERE 
            CAST(meds.order_inst_utc as TIMESTAMP) < labels.index_time
        AND
            TIMESTAMP_ADD(meds.order_inst_utc,
                          INTERVAL 24*{self.look_back_days} HOUR)
                          >= labels.index_time
        """
        query = add_create_or_append_logic(query, self.feature_table_id)
        query_job = self.client.query(query)
        query_job.result()
