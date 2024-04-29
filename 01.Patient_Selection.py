# Databricks notebook source
from pyspark.sql.functions import lit,countDistinct,col,regexp_replace,row_number,coalesce,date_add,lag,lead,when,datediff,avg,min as min_,max as max_,sequence,explode,collect_set,array_distinct,sum as sum_, collect_list, flatten,array,array_compact,count,sort_array,expr
from pyspark.sql.types import DateType
from pyspark.sql.window import Window

# COMMAND ----------

batch_id = spark.sql('''SELECT MAX(pt_batch_id) FROM us_comm_stg_prod.stg_hv_pharmacy_claims''').collect()[0][0]
str_ndc_ref = '`gdsi-oncanaloglib`.NDC_REF'
str_ndc_df = 'us_comm_stg_prod.stg_hv_ref_ndc'
str_hcpcs_ref = '`gdsi-oncanaloglib`.HCPCS_REF'
str_hcpcs_df = 'us_comm_stg_prod.stg_hv_ref_hcpcs'
str_icd_ref = '`gdsi-oncanaloglib`.ICD_REF'
str_icd_df = 'us_comm_stg_prod.stg_hv_ref_icd10_diagnosis'
str_ws1_ref = '`gdsi-oncanaloglib`.WS1_REF'
str_rx_claim = 'us_comm_stg_prod.stg_hv_pharmacy_claims'
str_mx_claim = 'src_mx_df'
pat_universe = '`gdsi-oncanaloglib`.HV_PAT_UNIVERSE_CLN'
last_version_key = spark.sql('''SELECT MAX(VERSION_KEY) FROM `gdsi-oncanaloglib`.HV_PAT_UNIVERSE_CLN''').collect()[0][0]
version_key = last_version_key + 1

# COMMAND ----------

pth = 's3://gilead-commercial-prd-us-west-2-curated/staging/adhoc/HV_ALL_TA/medical_claims/pt_batch_id=' + batch_id + '/'

# COMMAND ----------

#Read latest medical claims data directly from S3, repartition on service month
src_mx_df = spark.read.parquet(pth).repartition('part_mth_alt')
src_mx_df.createOrReplaceTempView('src_mx_df')

# COMMAND ----------

tumor_name = spark.sql("SELECT * FROM `gdsi-oncanaloglib`.TUMOR_SCOPE_REF").select('TumorGroup').rdd.flatMap(lambda x: x).collect()

# COMMAND ----------

for tumor_group in tumor_name:
    sqlContext.clearCache()
    spark.catalog.clearCache()
    # Get list of all NDC Codes linked with selected tumor type
    ndc_ref = spark.sql(''' SELECT A.* FROM 
                        (SELECT DISTINCT LPAD(ndc_code,11,'0') AS ndc_code, lower(level3) AS GENERIC_NAME, lower(level4) AS BRAND_NAME FROM {ndc_df}
                        WHERE pt_batch_id = {batch_id}) A
                        INNER JOIN 
                        (SELECT DISTINCT LPAD(NDC,11,'0') AS ndc_code, LOWER(Product) AS Product FROM {ndc_ref}) B
                        ON A.ndc_code = B.ndc_code
                        INNER JOIN
                        (SELECT DISTINCT LOWER(AnalogBrandName) AS Product FROM {ws1_ref}
                        WHERE LOWER(TumorGroup) LIKE '{tumor_group}') C
                        ON B.Product = C.Product
                        '''
                        .format(ndc_df=str_ndc_df, batch_id=batch_id, ndc_ref=str_ndc_ref, ws1_ref=str_ws1_ref,tumor_group=tumor_group))
    ndc_ref.createOrReplaceTempView('ndc_ref')

    ndc_list = ndc_ref.select('ndc_code').distinct().rdd.flatMap(lambda x: x).collect()
    ndc_list_fin = tuple(ndc_list)

    
    if len(ndc_list) > 1:
    #Insert unique patient IDs that used selected drugs anytime in history 
      spark.sql('''INSERT INTO {pat_universe}
                 (SELECT DISTINCT hvid, '{tumor}' AS TUMOR_GROUP, "RX" AS SOURCE,{version_key} AS VERSION_KEY FROM {rx_claim} WHERE ndc_code IN {ndc_list_fin} AND pt_batch_id = {batch_id}
                  AND logical_delete_reason IS NULL)'''.format(pat_universe=pat_universe,rx_claim=str_rx_claim,version_key=version_key,ndc_list_fin=ndc_list_fin, tumor=tumor_group, batch_id=batch_id))
    elif len(ndc_list) == 1:
      ndc = ndc_list[0]
      #Insert unique patient IDs that used selected drugs anytime in history 
      spark.sql('''INSERT INTO {pat_universe}
                 (SELECT DISTINCT hvid, '{tumor}' AS TUMOR_GROUP, "RX" AS SOURCE,{version_key} AS VERSION_KEY FROM {rx_claim} WHERE ndc_code = '{ndc}' AND pt_batch_id = {batch_id}
                  AND logical_delete_reason IS NULL)'''.format(pat_universe=pat_universe,rx_claim=str_rx_claim,version_key=version_key,ndc=ndc, tumor=tumor_group, batch_id=batch_id))
    
    #Identify all diagnosis codes associated with selected tumor type
    diag_ref = spark.sql('''SELECT A.* FROM
                        (SELECT DISTINCT REPLACE(code,'.','') AS diag_code,short_description, long_description 
                        FROM {icd_df}) A
                        INNER JOIN 
                        (SELECT DISTINCT REPLACE(ICD_code,'.','') AS diag_code FROM {icd_ref}
                        WHERE LOWER(tumor_group) = '{tumor}'
                        AND ICD_code IS NOT NULL AND ICD_code NOT IN ('',' ')) B
                        ON A.diag_code = B.diag_code'''.format(icd_df = str_icd_df, icd_ref = str_icd_ref,tumor = tumor_group))
    diag_ref.createOrReplaceTempView('diag_ref')
    diag_list = diag_ref.select('diag_code').distinct().rdd.flatMap(lambda x: x).collect()
    diag_list_fin = tuple(diag_list)

    if len(diag_list) > 1:
      #Insert unique patient IDs that used selected drugs anytime in history 
      spark.sql('''INSERT INTO {pat_universe}
                 (SELECT DISTINCT hvid, '{tumor}' AS TUMOR_GROUP, "MX" AS SOURCE,{version_key} AS VERSION_KEY FROM {mx_claim} WHERE diagnosis_code IN {diag_list_fin}
                 AND logical_delete_reason IS NULL)'''.format(pat_universe=pat_universe,mx_claim=str_mx_claim,version_key=version_key,diag_list_fin=diag_list_fin,tumor = tumor_group))
 
    elif len(diag_list) == 1:
      diag = diag_list[0]
      #Insert unique patient IDs that used selected drugs anytime in history 
      spark.sql('''INSERT INTO {pat_universe}
                 (SELECT DISTINCT hvid, '{tumor}' AS TUMOR_GROUP, "MX" AS SOURCE,{version_key} AS VERSION_KEY FROM {mx_claim} WHERE diagnosis_code = '{diag}'
                 AND logical_delete_reason IS NULL)'''.format(pat_universe=pat_universe,mx_claim=str_mx_claim,version_key=version_key,diag=diag,tumor = tumor_group))
      
    #Identify all procedure codes associated with selected tumor type
    proc_ref = spark.sql('''SELECT A.* FROM
                        (SELECT REPLACE(hcpcs_code,'.','') AS proc_code, long_description, short_description 
                        FROM {hcpcs_df}
                        WHERE pt_batch_id = {batch_id}) A
                        INNER JOIN 
                        (SELECT DISTINCT REPLACE(Code,'.','') AS proc_code, LOWER(BrandName) AS Product FROM {hcpcs_ref}) B
                        ON A.proc_code = B.proc_code
                        INNER JOIN
                        (SELECT DISTINCT LOWER(AnalogBrandName) AS Product FROM {ws1_ref}
                        WHERE LOWER(TumorGroup) LIKE '{tumor_group}') C
                        ON B.Product = C.Product'''.format(hcpcs_df = str_hcpcs_df, batch_id = batch_id, hcpcs_ref = str_hcpcs_ref, ws1_ref = str_ws1_ref, tumor_group=tumor_group))
    proc_ref.createOrReplaceTempView('proc_ref')     

    proc_list = proc_ref.select('proc_code').distinct().rdd.flatMap(lambda x: x).collect()
    proc_list_fin = tuple(proc_list)


    if len(proc_list) > 1:
    #Insert unique patient IDs that used selected drugs anytime in history 
      spark.sql('''INSERT INTO {pat_universe}
                (SELECT DISTINCT hvid, '{tumor}' AS TUMOR_GROUP, "PX" AS SOURCE,{version_key} AS VERSION_KEY FROM {mx_claim} WHERE procedure_code IN {proc_list_fin}
                  AND logical_delete_reason IS NULL)'''.format(pat_universe=pat_universe,mx_claim=str_mx_claim,version_key=version_key,proc_list_fin=proc_list_fin,tumor = tumor_group))
    
    elif len(proc_list) == 1:
      proc = proc_list[0]
      spark.sql('''INSERT INTO {pat_universe}
                (SELECT DISTINCT hvid, '{tumor}' AS TUMOR_GROUP, "PX" AS SOURCE,{version_key} AS VERSION_KEY FROM {mx_claim} WHERE procedure_code = '{proc}'
                  AND logical_delete_reason IS NULL)'''.format(pat_universe=pat_universe,mx_claim=str_mx_claim,version_key=version_key,proc=proc,tumor = tumor_group))
    
    


# COMMAND ----------


