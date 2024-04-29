# Databricks notebook source
spark.conf.set("spark.sql.shuffle.partitions","auto")
spark.conf.set("spark.databricks.io.cache.enabled", "true")

# COMMAND ----------

tumor_name = spark.sql("SELECT * FROM `gdsi-oncanaloglib`.TUMOR_SCOPE_REF").select('TumorGroup').rdd.flatMap(lambda x: x).collect()

# COMMAND ----------

from pyspark.sql.functions import lit,countDistinct,col,regexp_replace,row_number,coalesce,date_add,lag,lead,when,datediff,avg,min as min_,max as max_,sequence,explode,collect_set,array_distinct,sum as sum_, collect_list, flatten,array,array_compact,count,sort_array,expr,substring,concat,lower,concat_ws,array_except,size,dateadd,array_intersect
from pyspark.sql.types import DateType, IntegerType
from pyspark.sql.window import Window
import pyspark.sql.functions as F

# COMMAND ----------

batch_id = spark.sql('''SELECT MAX(pt_batch_id) FROM us_comm_stg_prod.stg_hv_pharmacy_claims''').collect()[0][0]
str_ndc_ref = '`gdsi-oncanaloglib`.NDC_REF'
str_icd_ref = '`gdsi-oncanaloglib`.ICD_REF'
str_hcpcs_ref = '`gdsi-oncanaloglib`.HCPCS_REF'
dos_ref = '`gdsi-oncanaloglib`.DOS_REF'
str_tumor_scope_ref = '`gdsi-oncanaloglib`.TUMOR_SCOPE_REF'

# COMMAND ----------

spark.sql('''TRUNCATE TABLE `gdsi-oncanaloglib`.REGIMEN_OUTPUT_DF ''')
for tumor_group in tumor_name:
    claims_df = spark.sql(''' SELECT A.*,lower(B.Tumor_Group) AS Tumor_Group FROM 
                      (SELECT CLAIM_ID,HV_ID,date_service,DAIGNOSIS_CODE,DAIGNOSIS_PRIORITY,procedure_code,NDC_CODE,dispensed_quantity,days_supply FROM `gdsi-oncanaloglib`.HV_CLAIMS_UNIVERSE_CLN WHERE TUMOR_GROUP = '{tumor_group}'  ) A
                      LEFT JOIN (SELECT * FROM {icd_ref} )B
                      ON A.DAIGNOSIS_CODE = REPLACE(B.ICD_code,'.','')'''.format(tumor_group=tumor_group,icd_ref=str_icd_ref)).withColumnRenamed('HV_ID','hvid')
    claims_df = claims_df.repartition('hvid') 
    dos_ref = '`gdsi-oncanaloglib`.DOS_REF'  
    icd_df = spark.sql('''SELECT DISTINCT REPLACE(code,'.','') AS DAIGNOSIS_CODE,lower(short_description) AS short_description, lower(long_description) AS long_description FROM us_comm_stg_prod.stg_hv_ref_icd10_diagnosis WHERE pt_batch_id = '{batch_id}' '''.format(batch_id=batch_id))
    #QC Step:
    if icd_df.count() != icd_df.select('DAIGNOSIS_CODE').distinct().count():
        raise Exception("Duplication in diagnosis code reference table")
    claims_df = claims_df.join(icd_df,'DAIGNOSIS_CODE','left')


    # # Indication Assignment
    # 1. Tumor type identified based on diagnosis code. If primary diagnosis code doesn't belong to tumor in scope secondary/tertiary ... diagnosis codes can be used
    # 2. Remove any diagnosis with phrase 'secondary' in description
    # 3. In case of multiple tumor types for patient, the earliest diagnosis is considered
    diag_df = claims_df.filter(claims_df.Tumor_Group.isNull()==False).filter(claims_df.Tumor_Group.isin(['',' '])==False).filter(claims_df.short_description.like('%secondary%')==False)
    claims_df = claims_df.withColumn('date_service',claims_df.date_service.cast(DateType()))
    diag_df = diag_df.withColumn('date_service',diag_df.date_service.cast(DateType()))


    #First tumor type for each patient
    w1 = Window.partitionBy('hvid','date_service').orderBy('DAIGNOSIS_PRIORITY')
    diag_df = diag_df.withColumn('diag_rnk',row_number().over(w1))
    indication_df = diag_df.filter(diag_df.diag_rnk == 1)
    w_pat = Window.partitionBy('hvid').orderBy('date_service')
    indication_df = indication_df.withColumn('IND_RN',row_number().over(w_pat))
    tumor_var = tumor_group.replace(" ", "_") + '_flag'
    ind_map = indication_df.filter(indication_df.IND_RN == 1).filter(indication_df.Tumor_Group == tumor_group).select('hvid').distinct().withColumn(tumor_var,lit(1))
    claims_df = claims_df.join(ind_map,'hvid','inner')


    # # Regimen calculation
    # Business Rule:
    # 1. All missing days of supply (MX) are considered 30 days (This is table driven and can be modified at product level in future)
    # 2. Grace period is considered as 30 days (This is table driven and can be modified at product level in future)
    # 3. Episode - Continuous use of a particular drug (including grace period) is considered as an episode
    # 4. Regimen - Overlapping use of multiple drugs on same day is considred as Regimen
    # 5. Any Regimen below 'X' days is dropped (15 days for this exercise, this is table driven and can be modified at indication level in future)
    ndc_ref = spark.sql('''SELECT * FROM {ndc_ref}'''.format(ndc_ref=str_ndc_ref)).withColumnRenamed('NDC','ndc_code')
    procedure_ref = spark.sql('''SELECT * FROM {hcpcs_ref}'''.format(hcpcs_ref=str_hcpcs_ref)).withColumnRenamed('Code','procedure_code')

    ndc_ref = ndc_ref.withColumn('Product_Name_1',when(ndc_ref.ChemoNonChemo == 'Chemo','Chemo')
                             .when(ndc_ref.AromataseInhibitior == 'Yes','AI').otherwise(ndc_ref.Product))\
                             .select('ndc_code','Product','Product_Name_1')
    procedure_ref = procedure_ref.withColumn('Product_Name_2',when(procedure_ref.ChemoNonChemo == 'Chemo','Chemo')
                                            .when(procedure_ref.BrandName.isNull()==False,procedure_ref.BrandName)
                                            .otherwise(procedure_ref.MoleculeName))\
                                            .select('procedure_code','BrandName','Product_Name_2')       

    claims_df = claims_df.join(ndc_ref,'ndc_code','left').join(procedure_ref,'procedure_code','left')
    claims_df = claims_df.filter((claims_df.Product_Name_1.isNull()==False)|(claims_df.Product_Name_2.isNull()==False))
    claims_df = claims_df.withColumn('Product_Fin',coalesce(claims_df.Product_Name_1,claims_df.Product_Name_2))                                 
    dos_ref = spark.sql('SELECT * FROM {dos_ref}'.format(dos_ref=dos_ref)).withColumnRenamed('days_supply','days_supply_ref').withColumnRenamed('Product_Name','Product_Fin')   

    #QC Step:
    if dos_ref.count() != dos_ref.select('Product_Fin').distinct().count():
        raise Exception("Duplication in days od supply reference table")    

    claims_df = claims_df.join(dos_ref,'Product_Fin','left') 
    claims_df = claims_df.na.fill({'days_supply_ref':30,'Grace_Period':30})    
    claims_df = claims_df.withColumn('days_supply',coalesce(claims_df.days_supply,claims_df.days_supply_ref))
    claims_df = claims_df.withColumn('days_supply',claims_df.days_supply.cast(IntegerType()))
    claims_df = claims_df.withColumn('Grace_Period',claims_df.Grace_Period.cast(IntegerType()))
    claims_df = claims_df.withColumn('SVC_END',date_add(col('date_service'),col('days_supply')+col('Grace_Period'))) 

    w2 = Window.partitionBy('hvid','Product_Fin').orderBy('date_service')
    claims_df = claims_df.withColumn('NXT_SVC',lead('date_service').over(w2))
    claims_df = claims_df.withColumn('LST_SVC_END',lag('SVC_END').over(w2))   
    claims_df = claims_df.withColumn('EPISODE_STRT_FLG',when(claims_df.LST_SVC_END.isNull(),1).when(claims_df.LST_SVC_END<claims_df.date_service,1).otherwise(0))
    claims_df = claims_df.withColumn('EPISODE_END_FLG',when(claims_df.NXT_SVC.isNull(),1).when(claims_df.SVC_END<claims_df.NXT_SVC,1).otherwise(0))     
    claims_df = claims_df.filter((claims_df.EPISODE_STRT_FLG == 1)|(claims_df.EPISODE_END_FLG == 1))    

    claims_df = claims_df.withColumn('EPISODE_END',lead('SVC_END').over(w2))
    claims_df = claims_df.withColumn('EPISODE_END',coalesce(claims_df.EPISODE_END,claims_df.SVC_END))
    claims_df = claims_df.filter(claims_df.EPISODE_STRT_FLG == 1)   
    claims_df = claims_df.withColumn('SVC_DAYS',datediff(claims_df.EPISODE_END,claims_df.date_service)+1)
    claims_df = claims_df.withColumn('ARR',sequence(lit(0),lit(claims_df.SVC_DAYS)))

    claims_df = claims_df.select('hvid','Product_Fin','date_service','ARR').withColumn('RN',explode('ARR'))
    claims_df = claims_df.withColumn('svc_date',date_add(col('date_service'),col('RN')))
    claims_df = claims_df.groupBy('hvid','svc_date').agg(array_distinct(collect_set('Product_Fin')).alias('Regimen'))
    claims_df = claims_df.withColumn('Regimen',array_distinct(array_compact(sort_array(col('Regimen')))))

    w3 = Window.partitionBy('hvid').orderBy('svc_date')
    claims_df = claims_df.withColumn('New_Reg',when(((claims_df.svc_date != dateadd(lag(claims_df.svc_date).over(w3),1)) |
                                                (claims_df.Regimen != lag(claims_df.Regimen).over(w3))),1)
                                 .otherwise(0))
    claims_df = claims_df.withColumn('New_Reg',sum_('New_Reg').over(w3))
    

    regimen_out = claims_df.groupBy('hvid','New_Reg').agg(min_('svc_date').alias('Regimen_Start'),sort_array(flatten(array_distinct(collect_list('Regimen')))).alias('Regimen'),countDistinct('svc_date').alias('Regimen_Days'))
    regimen_out = regimen_out.withColumn('Regimen',expr("transform(Regimen, x -> lower(x))"))
    regimen_out = regimen_out.withColumn('TUMOR',lit(tumor_group))

    tumor_scope_ref = spark.sql('''SELECT TumorGroup AS TUMOR,MIN_REG_LEN FROM {tumor_scope_ref}  '''.format(tumor_scope_ref=str_tumor_scope_ref))
    regimen_out = regimen_out.join(tumor_scope_ref,'TUMOR','left')
    regimen_out = regimen_out.filter(regimen_out.Regimen_Days >= regimen_out.MIN_REG_LEN)

    ws1_ref = spark.sql('''SELECT * FROM `gdsi-oncanaloglib`.WS1_REF_V2 ''')
    w1_map = ws1_ref.filter(lower(ws1_ref.TumorGroup) == tumor_group).select('AnalogBrandName','AnalogMoleculeName','Drug1','Drug2','Drug3','Drug4','Drug5','Indication','Patient_Segment','Line_of_therapy_approved')

    drug_var = tumor_group.replace(" ", "_") + '_drug_flag'
    w1_map = w1_map.withColumn('Regimen',array_distinct(sort_array(array_compact(array(w1_map.AnalogBrandName,w1_map.Drug2,w1_map.Drug3,w1_map.Drug4,w1_map.Drug5))))).withColumn(drug_var,lit(1)).select('Regimen','Indication','Line_of_therapy_approved','Patient_Segment','AnalogBrandName','AnalogMoleculeName',drug_var).withColumnRenamed('Indication','Biomarker')
    w1_map = w1_map.withColumn('Regimen',expr("transform(Regimen, x -> lower(x))"))

    ## braftovi is duplicated, check why?
    multi_ind = w1_map.groupBy('Regimen','Line_of_therapy_approved').agg(max_('Biomarker').alias('Biomarker'),max_('Patient_Segment').alias('Patient_Segment'),max_('AnalogMoleculeName').alias('AnalogMoleculeName'),countDistinct('Biomarker','Patient_Segment').alias('Multi'))
    multi_ind = multi_ind.withColumn('Biomarker_Fin',when(multi_ind.Multi >= 2,'Multi').otherwise(multi_ind.Biomarker))
    multi_ind = multi_ind.withColumn('Patient_Segment_Fin',when(multi_ind.Multi >= 2,'Multi').otherwise(multi_ind.Patient_Segment)) 

    multi_ind_dedup = multi_ind.dropDuplicates(['Regimen'])
    #QC Step:
    if multi_ind_dedup.count() != multi_ind_dedup.select('Regimen').distinct().count():
        raise Exception("Duplication in WS1 reference table") 

    regimen_out_matched = regimen_out.join(multi_ind_dedup.select('Regimen'), 'Regimen', 'inner')
    remaining_regimens = regimen_out.join(multi_ind_dedup, multi_ind_dedup['Regimen'] == regimen_out['Regimen'], 'left_anti')
    

    from itertools import combinations

    drugs = ['dexamethasone', 'chemo', 'radiation therapy', 'ai']
    all_combinations = []
    for r in range(1, len(drugs) + 1):
        all_combinations.extend(combinations(drugs, r))
    remaining_regimens = remaining_regimens.withColumn("regimen_original", remaining_regimens["Regimen"])
    regimen_out_matched = regimen_out_matched.withColumn("regimen_original", regimen_out_matched["Regimen"])    
    all_combinations_sorted = sorted(all_combinations, key=len)



    multi_ind_dedup = multi_ind_dedup.withColumn("Sorted_Regimen", sort_array(col("Regimen")))
    regimen_out_matched = regimen_out_matched.withColumn("Sorted_Regimen", sort_array(col("Regimen")))

    for combo in all_combinations_sorted:
        remaining_regimens1 = remaining_regimens.withColumn('Regimen', array_except(col('Regimen'), array(*[lit (item) for    item in combo])))
        remaining_regimens1 = remaining_regimens1.withColumn("Sorted_Regimen", sort_array(col("Regimen")))
        temp = remaining_regimens1.join(multi_ind_dedup.select('Sorted_Regimen'), 'Sorted_Regimen', 'inner')
        temp.createOrReplaceTempView('temp')
        temp= spark.sql('''SELECT Regimen,TUMOR,hvid,New_Reg,Regimen_Start,Regimen_Days,MIN_REG_LEN, regimen_original,   Sorted_Regimen from temp ''')       
        regimen_out_matched = regimen_out_matched.union(temp)       
        remaining_regimens = remaining_regimens1.join(multi_ind_dedup, multi_ind_dedup['Sorted_Regimen'] ==     remaining_regimens1['Sorted_Regimen'], 'left_anti')
        remaining_regimens1 = remaining_regimens1.withColumnRenamed("Regimen", "edited_regimen")
        remaining_regimens = remaining_regimens.withColumn("Regimen", remaining_regimens["regimen_original"])
    
    
    remaining_regimens.createOrReplaceTempView('remaining_regimens')
    regimen_out.createOrReplaceTempView('regimen_out')
    remaining_regimens = spark.sql ( ''' select Regimen,  TUMOR, hvid, New_Reg, Regimen_Start, Regimen_Days, MIN_REG_LEN, 
     regimen_original from remaining_regimens ''')
    remaining_regimens.createOrReplaceTempView('remaining_regimens')
    remaining_regimens = remaining_regimens.withColumn("Sorted_Regimen", sort_array(col("Regimen")))
    regimen_out = regimen_out_matched


    # # Line of Therapy
    # If last regimen is greater than predefined threshold and a new product is added to the regimen, LoT progresses
    new_lot_days = spark.sql('''SELECT DISTINCT(NEW_LOT_DAYS) AS NEW_LOT_DAYS FROM {tumor_scope_ref} WHERE TumorGroup = '{tumor_group}' '''.format    (tumor_scope_ref=str_tumor_scope_ref,tumor_group=tumor_group)).collect()[0][0]
    w = Window.partitionBy('hvid').orderBy('Regimen_Start')
    regimen_out = regimen_out.withColumn('NEW_LOT_FLAG',when((lag(col('Regimen_Days')).over(w) >= new_lot_days)
                                                                 &(size(array_except(regimen_out.Regimen,lag(regimen_out.Regimen).over(w))) !=0),1)
                                         .when(size(array_intersect(regimen_out.Regimen,lag(regimen_out.Regimen).over(w))) == 0,1).otherwise(0))
    regimen_out = regimen_out.withColumn('LoT',sum_('NEW_LOT_FLAG').over(w))
    regimen_out = regimen_out.withColumn('LoT',col('LoT')+1)

    
    condition = when(regimen_out['LoT'] >=2, '2L+').otherwise('1L')
    
    regimen_out = regimen_out.withColumn('LoT_final', condition)
    remaining_regimens = remaining_regimens.withColumn("NEW_LOT_FLAG", lit(None).cast(IntegerType())) \
       .withColumn("LoT", lit(None).cast(IntegerType())) \
       .withColumn("LoT_final", lit(None).cast("string"))    
    regimen_out = regimen_out.union(remaining_regimens)  

    multi_ind = multi_ind.withColumn("Sorted_Regimen", sort_array(col("Regimen")))

    regimen_out = regimen_out.join(multi_ind, (regimen_out['Sorted_Regimen'] == multi_ind['Sorted_Regimen']) & (regimen_out['LoT_final'] == multi_ind['Line_of_therapy_approved']), how='left').select(regimen_out['*'], multi_ind['Biomarker_Fin'], multi_ind['Patient_Segment_Fin'], multi_ind['AnalogMoleculeName'])

    regimen_out = regimen_out.withColumn(
    'match_tag',
    when(
        (col('Biomarker_Fin').isNotNull()) & (col('Patient_Segment_Fin').isNotNull()),
        'match'
    ).otherwise('other'))

    regimen_out = regimen_out.withColumn('YR_MNTH',substring(regimen_out.Regimen_Start,1,7))
   

    fin_out = regimen_out.groupBy('TUMOR','Regimen','YR_MNTH','LoT','Regimen_Start','Biomarker_Fin','Patient_Segment_Fin','AnalogMoleculeName').agg(sum_('Regimen_Days').alias('Regimen_Days'))
    fin_out = fin_out.withColumn('Regimen',array_distinct(array_compact(sort_array(col('Regimen')))))
    fin_out = fin_out.withColumn('Regimen', concat_ws("-", col('Regimen')))
    fin_out.createOrReplaceTempView('fin_out')

    print(tumor_group)
    spark.sql('''INSERT INTO `gdsi-oncanaloglib`.REGIMEN_OUTPUT_DF SELECT * FROM fin_out''')
    

# COMMAND ----------


