"""
Cohort definitions using STARR (STRIDE) data
"""
from healthrex_ml.cohorts.cohort import CohortBuilder

class CBCWithDifferentialCohortWithValue(CohortBuilder):
    """
    Defines a cohort and labels (result values) for CBC with differential models, 2000 observations randomly sampled per year
    """

    def __init__(self, client, dataset_name, table_name,
                 working_project_id='mining-clinical-decisions'):
        """
        Initializes dataset_name and table_name for where cohort table will be
        saved on bigquery
        """
        super().__init__(client, dataset_name,
              table_name, working_project_id)

    def __call__(self):
        """
        Function that constructs a cohort table for predicting cbc with
        differential results. Done with SQL logic where possible
        """
        query = f"""
        CREATE OR REPLACE TABLE 
        {self.project_id}.{self.dataset_name}.{self.table_name}
        AS (
        WITH cbcd_lab_results as (
        SELECT DISTINCT
            anon_id,
            order_id_coded,
            order_time_utc as index_time,
            ordering_mode,
            base_name,
            ord_num_value label
        FROM 
            som-nero-phi-jonc101.shc_core_2021.lab_result
        WHERE 
            # Note no inpatient results where proc code was LABCBCD
            UPPER(group_lab_name) = 'CBC WITH DIFFERENTIAL'
        AND
            ((base_name='HCT' AND reference_unit='%') 
            OR (base_name='HGB' AND reference_unit='g/dL') 
            OR (base_name='PLT' AND reference_unit IN ('K/uL', 'x10E3/uL', 'Thousand/uL')) 
            OR (base_name='WBC' AND reference_unit IN ('K/uL', 'x10E3/uL', 'Thousand/uL')))
        AND 
            EXTRACT(YEAR FROM order_time_utc) BETWEEN 2015 and 2021
        AND
            ord_num_value IS NOT NULL
        ),
        # Pivot lab result to wide
        cohort_wide as (
            SELECT 
                * 
            FROM 
                cbcd_lab_results
            PIVOT (
                MAX(label) as label -- should be max of one value or no value 
                FOR base_name in ('WBC', 'PLT', 'HCT', 'HGB')
            )
            WHERE 
                -- only keep labs where all three components result
                label_WBC is not NULL AND
                label_PLT is not NULL AND
                label_HCT is not NULL AND
                label_HGB is not NULL
        )
        
        SELECT 
            anon_id, order_id_coded as observation_id, index_time, 
            ordering_mode, label_WBC, label_PLT, label_HCT, label_HGB
        FROM 
            (SELECT *,
                ROW_NUMBER() OVER  (PARTITION BY EXTRACT(YEAR FROM index_time)
                                    ORDER BY RAND()) 
                        AS seqnum
            FROM cohort_wide 
            ) 
        WHERE
            seqnum <= 2000
        )
        
        """
        query_job = self.client.query(query)
        query_job.result()


# local data at /deep/group/aihc/win23/EHR/CBC_NoSampling_uncleaned.pkl
class CBCWithDifferentialCohortWithValueNoSampling(CohortBuilder):
    """
    Defines a cohort and labels (result values) for CBC with differential models, with no sampling
    """

    def __init__(self, client, dataset_name, table_name,
                 working_project_id='mining-clinical-decisions'):
        """
        Initializes dataset_name and table_name for where cohort table will be
        saved on bigquery
        """
        super().__init__(client, dataset_name,
              table_name, working_project_id)

    def __call__(self):
        """
        Function that constructs a cohort table for predicting cbc with
        differential results. Done with SQL logic where possible
        """
        query = f"""
        CREATE OR REPLACE TABLE 
        {self.project_id}.{self.dataset_name}.{self.table_name}
        AS (
        WITH cbcd_lab_results as (
        SELECT DISTINCT
            anon_id,
            order_id_coded,
            order_time_utc as index_time,
            ordering_mode,
            base_name,
            ord_num_value label
        FROM 
            som-nero-phi-jonc101.shc_core_2021.lab_result
        WHERE 
            # Note no inpatient results where proc code was LABCBCD
            UPPER(group_lab_name) = 'CBC WITH DIFFERENTIAL'
        AND
            ((base_name='HCT' AND reference_unit='%') 
            OR (base_name='HGB' AND reference_unit='g/dL') 
            OR (base_name='PLT' AND reference_unit IN ('K/uL', 'x10E3/uL', 'Thousand/uL')) 
            OR (base_name='WBC' AND reference_unit IN ('K/uL', 'x10E3/uL', 'Thousand/uL')))
        AND 
            EXTRACT(YEAR FROM order_time_utc) BETWEEN 2015 and 2021
        AND
            ord_num_value IS NOT NULL
        ),
        # Pivot lab result to wide
        cohort_wide as (
            SELECT 
                * 
            FROM 
                cbcd_lab_results
            PIVOT (
                MAX(label) as label -- should be max of one value or no value 
                FOR base_name in ('WBC', 'PLT', 'HCT', 'HGB')
            )
            WHERE 
                -- only keep labs where all three components result
                label_WBC is not NULL AND
                label_PLT is not NULL AND
                label_HCT is not NULL AND
                label_HGB is not NULL
        )
        
        SELECT 
            anon_id, order_id_coded as observation_id, index_time, 
            ordering_mode, label_WBC, label_PLT, label_HCT, label_HGB
        FROM 
            (SELECT *,
                ROW_NUMBER() OVER  (PARTITION BY EXTRACT(YEAR FROM index_time)
                                    ORDER BY RAND()) 
                        AS seqnum
            FROM cohort_wide 
            )
        )
        
        """
        query_job = self.client.query(query)
        query_job.result()
