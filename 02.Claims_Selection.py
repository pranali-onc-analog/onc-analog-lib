# Databricks notebook source
from pyspark.sql.functions import lit,countDistinct,col,regexp_replace,row_number,coalesce,date_add,lag,lead,when,datediff,avg,min as min_,max as max_,sequence,explode,collect_set,array_distinct,sum as sum_, collect_list, flatten,array,array_compact,count,sort_array,expr
from pyspark.sql.types import DateType
from pyspark.sql.window import Window

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions","auto")

# COMMAND ----------

batch_id = spark.sql('''SELECT MAX(pt_batch_id) FROM us_comm_stg_prod.stg_hv_pharmacy_claims''').collect()[0][0]
str_ndc_ref = '`gdsi-oncanaloglib`.NDC_REF'
str_ndc_df = 'us_comm_stg_prod.stg_hv_ref_ndc'
str_hcpcs_ref = '`gdsi-oncanaloglib`.HCPCS_REF'
str_hcpcs_df = 'us_comm_stg_prod.stg_hv_ref_hcpcs'
str_icd_ref = '`gdsi-oncanaloglib`.ICD_REF'
str_icd_df = 'us_comm_stg_prod.stg_hv_ref_icd10_diagnosis'
str_ws1_ref = '`gdsi-oncanaloglib`.WS1_REF'
str_rx_claim = 'src_rx_df'
str_mx_claim = 'src_mx_df'
version_key = spark.sql('''SELECT MAX(VERSION_KEY) FROM `gdsi-oncanaloglib`.HV_PAT_UNIVERSE_CLN ''').collect()[0][0]

# COMMAND ----------

#Read latest medical claims data directly from S3, repartition on service month
mx_pth = 's3://gilead-commercial-prd-us-west-2-curated/staging/adhoc/HV_ALL_TA/medical_claims/pt_batch_id=' + batch_id + '/'
src_mx_df = spark.read.parquet(mx_pth).repartition('part_mth_alt')
src_mx_df.createOrReplaceTempView('src_mx_df')

#Read latest pharmacy claims data directly from S3, repartition on service month
rx_pth = 's3://gilead-commercial-prd-us-west-2-curated/staging/adhoc/HV_ALL_TA/pharmacy_claims/pt_batch_id=' + batch_id + '/'
src_rx_df = spark.read.parquet(rx_pth).repartition('part_mth_alt')
src_rx_df.createOrReplaceTempView('src_rx_df')

# COMMAND ----------

tumor_name = spark.sql("SELECT * FROM `gdsi-oncanaloglib`.TUMOR_SCOPE_REF").select('TumorGroup').rdd.flatMap(lambda x: x).collect()

# COMMAND ----------

spark.sql('''TRUNCATE TABLE `gdsi-oncanaloglib`.HV_CLAIMS_UNIVERSE_CLN ''')
for tumor_group in tumor_name:
    sqlContext.clearCache()
    spark.catalog.clearCache()
    
    #select unique patient for the tumor
    print(tumor_group)
    unq_pat = spark.sql('''SELECT DISTINCT HV_ID AS hvid FROM `gdsi-oncanaloglib`.HV_PAT_UNIVERSE_CLN WHERE TUMOR_GROUP ='{tumor_group}' AND VERSION_KEY = {version_key} AND HV_ID IS NOT NULL'''.format(tumor_group = tumor_group,version_key=version_key))
    unq_pat.createOrReplaceTempView('unq_pat')

    unq_pat_list = unq_pat.select('hvid').distinct().rdd.flatMap(lambda x: x).collect()
    unq_pat_list_fin = tuple(unq_pat_list)

    #insert rx records in clean claims table
    if len(unq_pat_list) > 1:
        print('Inserting pharmacy claims')
        spark.sql('''INSERT INTO `gdsi-oncanaloglib`.HV_CLAIMS_UNIVERSE_CLN
                  (SELECT claim_id,hvid,date_service,REPLACE(diagnosis_code,'.','') AS diagnosis_code,NULL AS diagnosis_priority,procedure_code,ndc_code,dispensed_quantity,days_supply,'{tumor_group}','RX' AS Source  FROM {rx_claim} A
                  WHERE EXISTS (SELECT hvid FROM unq_pat B WHERE B.hvid = A.hvid)
                  AND logical_delete_reason IS NULL)'''.format(rx_claim = str_rx_claim,tumor_group=tumor_group,unq_pat=unq_pat_list_fin))
    
        #insert mx records in clean claims table
        print('Inserting medical claims')
        spark.sql('''INSERT INTO `gdsi-oncanaloglib`.HV_CLAIMS_UNIVERSE_CLN
                  (SELECT claim_id,hvid,date_service,REPLACE(diagnosis_code,'.','') AS diagnosis_code,diagnosis_priority,procedure_code,ndc_code,NULL AS dispensed_quantity,NULL AS days_supply,'{tumor_group}',
                  'MX' AS SOURCE FROM {mx_claim} A
                  WHERE EXISTS (SELECT hvid FROM unq_pat B WHERE B.hvid = A.hvid)
                  AND logical_delete_reason IS NULL)'''.format(mx_claim = str_mx_claim,unq_pat_list_fin=unq_pat_list_fin,tumor_group = tumor_group))
