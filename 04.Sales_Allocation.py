# Databricks notebook source
# MAGIC %md
# MAGIC Read sales data

# COMMAND ----------

import pyspark.sql.functions as f
from pyspark.sql.types import FloatType,StructType,StructField,StringType,IntegerType

# COMMAND ----------

qtr_sales = spark.sql('''SELECT * FROM `gdsi-oncanaloglib`.SALES_DF''')
qtr_sales = qtr_sales.withColumnRenamed('SALES','TOT_SALES')
qtr_sales = qtr_sales.withColumn('ANALOG',f.lower(qtr_sales.PRODUCT))
#QC Step:
if qtr_sales.count() != qtr_sales.select('ANALOG','Calendar_Quarter').distinct().count():
    raise Exception("Duplication in national sales data")

# COMMAND ----------

max_qt = qtr_sales.agg(f.max('Calendar_Quarter')).collect()[0][0]

# COMMAND ----------

tumor_name = spark.sql("SELECT * FROM `gdsi-oncanaloglib`.TUMOR_SCOPE_REF ").select('TumorGroup').rdd.flatMap(lambda x: x).collect()
schema = StructType([
    StructField("TUMOR",StringType(),True),
    StructField("REGIMEN",StringType(),True),
    StructField("Calendar_Quarter",StringType(),True),
    StructField("LoT_Tag",StringType(),True),
    StructField("Mono_Doublet_Triplet",StringType(),True),
    StructField("BIOMARKER",StringType(),True),
    StructField("PATIENT_SEGMENT",StringType(),True),
    StructField("AnalogMoleculeName",StringType(),True),
    StructField("ANALOG",StringType(),True),
    StructField("REGIMEN_DAYS",StringType(),True)
])
out_df = sqlContext.createDataFrame(sc.emptyRDD(), schema)
for tumor_group in tumor_name:
    #print(tumor_group)
    reg_df = spark.sql('''SELECT * FROM `gdsi-oncanaloglib`.REGIMEN_OUTPUT_DF WHERE TUMOR = '{tumor_group}' '''.format(tumor_group=tumor_group))
    reg_df = reg_df.withColumn('REGIMEN_DAYS',reg_df.REGIMEN_DAYS.cast(IntegerType()))
    reg_df = reg_df.withColumn('ARR',f.sequence(f.lit(0),f.lit(reg_df.REGIMEN_DAYS)))
    reg_df = reg_df.withColumn('RN',f.explode('ARR'))
    reg_df = reg_df.drop('ARR')
    reg_df = reg_df.withColumn('SVC_DT',f.date_add(reg_df.Regimen_Start,f.col('RN')))
    ws1_ref = spark.sql('''SELECT DISTINCT LOWER(AnalogBrandName) AS AnalogBrandName FROM `gdsi-oncanaloglib`.WS1_REF_V2 WHERE LOWER(TumorGroup) = '{tumor_group}' '''.format(tumor_group=tumor_group)).select('AnalogBrandName').rdd.flatMap(lambda x: x).collect()
    reg_df = reg_df.withColumn('ANALOG_REF',f.array(*[f.lit(element) for element in ws1_ref]))
    reg_df = reg_df.withColumn('REG_ARR',f.split(reg_df.REGIMEN,'-'))
    reg_df = reg_df.withColumn('REG_CT',f.size(reg_df.REG_ARR))
    reg_df = reg_df.withColumn('ANALOG',f.array_intersect(reg_df.REG_ARR,reg_df.ANALOG_REF))
    reg_df = reg_df.withColumn('ANALOG_CT',f.size(reg_df.ANALOG))
    reg_df = reg_df.drop('ANALOG_REF')
    reg_df = reg_df.withColumn('QTR',f.when(f.substring(reg_df.YR_MNTH,6,2).isin([1,2,3,'01','02','03']),1)
                           .when(f.substring(reg_df.YR_MNTH,6,2).isin([4,5,6,'04','05','06']),2)
                           .when(f.substring(reg_df.YR_MNTH,6,2).isin([7,8,9,'07','08','09']),3)
                           .when(f.substring(reg_df.YR_MNTH,6,2).isin([10,11,12,'10','11','12']),4)
                           .otherwise(None))
    reg_df = reg_df.withColumn('Calendar_Quarter',f.concat(f.substring(reg_df.YR_MNTH,1,4),f.lit(' '),f.lit('Q'),reg_df.QTR))     
    reg_df = reg_df.withColumn('LoT_Tag',f.when(reg_df.LoT ==1,'1L').when(reg_df.LoT >=2,'2L+').otherwise(reg_df.LoT))
    reg_df = reg_df.withColumn('Mono_Doublet_Triplet',f.when(reg_df.REG_CT == 1,'Mono')
                                                       .when(reg_df.REG_CT == 2,'Doublet')
                                                       .when(reg_df.REG_CT >= 3,'Triplet').otherwise('NA'))

    reg_rollup = reg_df.groupBy('TUMOR','REGIMEN','Calendar_Quarter','LoT_Tag','Mono_Doublet_Triplet','BIOMARKER','PATIENT_SEGMENT','AnalogMoleculeName','ANALOG','ANALOG_CT').agg(f.count('SVC_DT').alias('REGIMEN_DAYS'))

    tumor_df = reg_rollup.filter(reg_rollup.ANALOG_CT >= 1).select('TUMOR','REGIMEN','Calendar_Quarter','LoT_Tag','Mono_Doublet_Triplet','BIOMARKER','PATIENT_SEGMENT','AnalogMoleculeName','ANALOG','REGIMEN_DAYS').withColumn('ANALOG',f.explode('ANALOG'))
    out_df = out_df.unionAll(tumor_df)

all_analog = out_df.select('ANALOG','REGIMEN_DAYS','Calendar_Quarter').groupBy('Calendar_Quarter','ANALOG').agg(f.sum('REGIMEN_DAYS').alias('TOT_DAYS'))
out_df = out_df.join(all_analog,['ANALOG','Calendar_Quarter'],'left')
out_df = out_df.withColumn('PROP',out_df.REGIMEN_DAYS/out_df.TOT_DAYS)
out_df = out_df.join(qtr_sales.select('ANALOG','Calendar_Quarter','TOT_SALES'),['ANALOG','Calendar_Quarter'],'left')
out_df = out_df.withColumn('SALES',out_df.TOT_SALES*out_df.PROP)
out_df = out_df.withColumn('BIOMARKER',f.lower(out_df.BIOMARKER))
out_df = out_df.withColumn('PATIENT_SEGMENT',f.lower(out_df.PATIENT_SEGMENT))
out_df = out_df.na.fill({'SALES':0})

# COMMAND ----------

hist_sales = spark.sql('''SELECT * FROM `gdsi-oncanaloglib`.Hist_sales_final_output''')
hist_sales = hist_sales.withColumnsRenamed({'AnalogBrandName':'ANALOG','MonoDoubletTriplet':'Mono_Doublet_Triplet','Line_of_therapy_approved':'LoT_Tag','Indication':'BIOMARKER','Patient_Segment':'PATIENT_SEGMENT','Sales_final':'SALES'})
hist_sales = hist_sales.withColumn('ANALOG',f.lower(hist_sales.ANALOG))
hist_sales = hist_sales.withColumn('BIOMARKER',f.lower(hist_sales.BIOMARKER))
hist_sales = hist_sales.withColumn('PATIENT_SEGMENT',f.lower(hist_sales.PATIENT_SEGMENT))
hist_sales = hist_sales.withColumn('Calendar_Quarter',f.concat(f.substring(hist_sales.Quarter,4,4),f.lit(' '),f.substring(hist_sales.Quarter,1,2)))
hist_sales = hist_sales.withColumn('REGIMEN',hist_sales.AnalogMoleculeName)
hist_sales = hist_sales.withColumn('REGIMEN_DAYS',f.lit(None))
hist_sales = hist_sales.withColumn('TOT_DAYS',f.lit(None))
hist_sales = hist_sales.withColumn('PROP',f.lit(None))
hist_sales = hist_sales.withColumn('TOT_SALES',f.lit(None))
tumor_ref = spark.sql('''SELECT DISTINCT LOWER(TumorGroup) AS TUMOR, LOWER(Indication) AS BIOMARKER FROM `gdsi-oncanaloglib`.ws1_ref_v2''')
hist_sales = hist_sales.join(tumor_ref,'BIOMARKER','left')
hist_sales = hist_sales.withColumn('AnalogMoleculeName',f.regexp_replace(hist_sales.AnalogMoleculeName,"  "," "))
hist_sales = hist_sales.withColumn('PATIENT_SEGMENT',f.regexp_replace(hist_sales.PATIENT_SEGMENT,"  "," "))
hist_sales = hist_sales.select('ANALOG', 'Calendar_Quarter', 'TUMOR', 'REGIMEN', 'LoT_Tag', 'Mono_Doublet_Triplet', 'BIOMARKER', 'PATIENT_SEGMENT', 'AnalogMoleculeName', 'REGIMEN_DAYS', 'TOT_DAYS', 'PROP', 'TOT_SALES', 'SALES')

# COMMAND ----------

cols = out_df.columns
out_df = out_df.select(cols).unionAll(hist_sales)

# COMMAND ----------

roa_df = spark.read.format('csv') \
  .option("inferSchema", 'true') \
  .option("header", 'true') \
  .option("sep", ',') \
  .load('/FileStore/tables/ROA_DF.csv')
#QC Step:
if roa_df.count() != roa_df.select('ANALOG').distinct().count():
    raise Exception("Duplication in ROA reference file")
roa_df = roa_df.withColumn('ANALOG',f.lower(roa_df.ANALOG))
out_df = out_df.join(roa_df,'ANALOG','left')

# COMMAND ----------

moa_df = spark.read.format('csv') \
  .option("inferSchema", 'true') \
  .option("header", 'true') \
  .option("sep", ',') \
  .load('/FileStore/tables/MOA_REF.csv')

# COMMAND ----------

moa_df = moa_df.withColumn('TumorGroup',f.lower(moa_df.TumorGroup))
moa_df = moa_df.withColumnsRenamed({'Line_of_therapy_approved':'LoT_Tag','TumorGroup':'TUMOR','Indication':'BIOMARKER','Patient_Segment':'PATIENT_SEGMENT'}).select('TUMOR','AnalogMoleculeName','BIOMARKER','PATIENT_SEGMENT','LoT_Tag','Class_of_MoA').distinct()
moa_df = moa_df.withColumn('BIOMARKER',f.lower(moa_df.BIOMARKER))
moa_df = moa_df.withColumn('PATIENT_SEGMENT',f.lower(moa_df.PATIENT_SEGMENT))

# COMMAND ----------

#QC Step:
if moa_df.count() != moa_df.select('TUMOR','AnalogMoleculeName','BIOMARKER','PATIENT_SEGMENT','LoT_Tag').distinct().count():
    raise Exception("Duplication in MOA reference file")

# COMMAND ----------

out_df = out_df.join(moa_df,['TUMOR','AnalogMoleculeName','BIOMARKER','PATIENT_SEGMENT','LoT_Tag'],'left')

# COMMAND ----------

out_df.filter((out_df.Class_of_MoA.isNull())).filter(out_df.BIOMARKER.isNull()==False).filter(out_df.BIOMARKER != 'multi').select('TUMOR','AnalogMoleculeName','BIOMARKER','PATIENT_SEGMENT','LoT_Tag').distinct().display()

# COMMAND ----------

epi_ref = spark.read.format('csv') \
  .option("inferSchema", 'true') \
  .option("header", 'true') \
  .option("sep", ',') \
  .load('/FileStore/tables/EPI_REF.csv')

# COMMAND ----------

epi_ref = epi_ref.withColumn('TumorGroup',f.lower(epi_ref.TumorGroup))
epi_ref = epi_ref.withColumn('Biomarker',f.lower(epi_ref.Biomarker))
epi_ref = epi_ref.withColumn('Patient_Segment',f.lower(epi_ref.Biomarker))

# COMMAND ----------

out_df = out_df.na.fill({'SALES':0})
multi_sales_test = out_df.agg(f.sum('SALES')).collect()[0][0]

# COMMAND ----------

epi_ref = epi_ref.withColumnsRenamed({'TumorGroup':'TUMOR','Regimen':'REGIMEN','Line_of_therapy_approved':'LoT_Tag','Biomarker':'BIOMARKER_REF','Patient_Segment':'Patient_Segment_Ref'}).dropDuplicates()

# COMMAND ----------

epi_ref = epi_ref.drop('Patient_Segment_epi_prevalence')

# COMMAND ----------

out_df_multi = out_df.filter(out_df.BIOMARKER == 'multi')
out_df_org = out_df.filter((out_df.BIOMARKER.isNull())|(out_df.BIOMARKER != 'multi'))

# COMMAND ----------

out_df_multi = out_df_multi.join(epi_ref,['TUMOR','REGIMEN','LoT_Tag'],'left')

# COMMAND ----------

out_df_multi = out_df_multi.withColumn('SALES_TEST',f.when(out_df_multi.Split.isNull()==False,out_df_multi.Split*out_df_multi.SALES).otherwise(out_df_multi.SALES))

# COMMAND ----------

out_df_multi = out_df_multi.withColumn('BIOMARKER',f.when((out_df_multi.BIOMARKER == 'multi')&(out_df_multi.BIOMARKER_REF.isNull()==False),out_df_multi.BIOMARKER_REF).otherwise(out_df_multi.BIOMARKER))
out_df_multi = out_df_multi.withColumn('PATIENT_SEGMENT',f.when((out_df_multi.PATIENT_SEGMENT == 'multi')&(out_df_multi.Patient_Segment_Ref.isNull()==False),out_df_multi.Patient_Segment_Ref).otherwise(out_df_multi.PATIENT_SEGMENT))
out_df_multi = out_df_multi.withColumn('SALES',f.when(out_df_multi.SALES_TEST.isNull()==False,out_df_multi.SALES_TEST).otherwise(out_df_multi.SALES))

# COMMAND ----------

cols = out_df.columns
out_df = out_df_org.select(cols).unionAll(out_df_multi.select(cols))

# COMMAND ----------

multi_sales_test3 = out_df.agg(f.sum('SALES')).collect()[0][0]

# COMMAND ----------

print(multi_sales_test)
print(multi_sales_test3)

# COMMAND ----------

# Additional measures 
out_df = out_df.na.fill({'SALES':0})
ms_map = out_df.groupBy('BIOMARKER','LoT_Tag','Calendar_Quarter').agg(f.sum('SALES').alias('MS_REF'))
out_df = out_df.join(ms_map,['BIOMARKER','LoT_Tag','Calendar_Quarter'],'left')
out_df = out_df.withColumn('MARKET_SHARE',out_df.SALES/out_df.MS_REF)
peak_share_map = out_df.groupBy('ANALOG','REGIMEN','BIOMARKER','LoT_Tag').agg(f.max('MARKET_SHARE').alias('PEAK_SHARE'))
out_df = out_df.join(peak_share_map,['ANALOG','REGIMEN','BIOMARKER','LoT_Tag'],'left')
out_df = out_df.withColumn('YEAR',f.substring(out_df.Calendar_Quarter,1,4))

# COMMAND ----------

out_df = out_df.withColumn("Mono_Doublet_Triplet", f.initcap(f.col('Mono_Doublet_Triplet')))

# COMMAND ----------

out_df.createOrReplaceTempView('out_df')
spark.sql('''CREATE OR REPLACE TABLE `gdsi-oncanaloglib`.WS2_OUTPUT_RAW AS SELECT * FROM out_df''')

# COMMAND ----------

WS2_OUT = spark.sql('''SELECT ANALOG,AnalogMoleculeName,REGIMEN,TUMOR,LoT_Tag,Mono_Doublet_Triplet,Calendar_Quarter, BIOMARKER,PATIENT_SEGMENT,SALES,YEAR,Route_of_Administration AS MOA,Class_of_MoA,MARKET_SHARE,PEAK_SHARE FROM `gdsi-oncanaloglib`.WS2_OUTPUT_RAW WHERE BIOMARKER IS NOT NULL AND Calendar_Quarter <= '{max_qt}' '''.format(max_qt=max_qt))
WS2_OUT.repartition(1).write.format("com.databricks.spark.csv").mode('overwrite').option("header","true").option("inferSchema", "true").save("s3://gilead-edp-commercial-dev-us-west-2-gdsi-oncanaloglib/WS2_OUT")

# COMMAND ----------

WS2_OUT_PIVOT_MS = out_df.filter(out_df.BIOMARKER.isNull()==False).filter(out_df.Calendar_Quarter <= max_qt).groupBy( 'ANALOG', 'AnalogMoleculeName' , 'TUMOR', 'LoT_Tag' , 'Mono_Doublet_Triplet' , 'BIOMARKER' ,	'PATIENT_SEGMENT', 'Route_of_Administration' ,'Class_of_MoA').pivot('Calendar_Quarter').agg(f.avg('MARKET_SHARE'))
WS2_OUT_PIVOT_MS.repartition(1).write.format("com.databricks.spark.csv").mode('overwrite').option("header","true").option("inferSchema", "true").save("s3://gilead-edp-commercial-dev-us-west-2-gdsi-oncanaloglib/WS2_OUT_PIVOT_MS")

# COMMAND ----------

WS2_OUT_PIVOT_SALES = out_df.filter(out_df.BIOMARKER.isNull()==False).filter(out_df.Calendar_Quarter <= max_qt).groupBy( 'ANALOG', 'AnalogMoleculeName' , 'TUMOR', 'LoT_Tag' , 'Mono_Doublet_Triplet' , 'BIOMARKER' ,	'PATIENT_SEGMENT', 'Route_of_Administration' ,'Class_of_MoA').pivot('Calendar_Quarter').agg(f.avg('SALES'))
WS2_OUT_PIVOT_SALES.repartition(1).write.format("com.databricks.spark.csv").mode('overwrite').option("header","true").option("inferSchema", "true").save("s3://gilead-edp-commercial-dev-us-west-2-gdsi-oncanaloglib/WS2_OUT_PIVOT_SALES")
