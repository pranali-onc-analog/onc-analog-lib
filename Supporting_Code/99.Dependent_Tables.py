# Databricks notebook source
from pyspark.sql.functions import col, lit, concat, substring, regexp_replace, sum as sum_, lower,countDistinct
from pyspark.sql.types import FloatType,StructType,StructField,StringType,IntegerType
import pyspark.sql.functions as f

# COMMAND ----------

sales_df1 = spark.read.format('csv') \
  .option("inferSchema", 'true') \
  .option("header", 'true') \
  .option("sep", ',') \
  .load('/FileStore/tables/SALES_98_08.csv')
sales_df1 = sales_df1.filter(sales_df1.Country == 'US')

# COMMAND ----------

cols_all = sales_df1.columns
req_cols = [col for col in cols_all if 'mnf' in col]
schema = StructType([
    StructField("PRODUCT",StringType(),True),
    StructField("SALES",StringType(),True),
    StructField("Calendar_Quarter",StringType(),True)])
out_df1 = sqlContext.createDataFrame(sc.emptyRDD(), schema)
for col in req_cols:
    qtr = str(int(col[14:16])/3)[0:1]
    yr = col[17:21]
    qtr_str = yr+' Q'+qtr
    df = sales_df1.select('PRODUCT',col).withColumn('Calendar_Quarter',lit(qtr_str))
    out_df1 = out_df1.union(df)


# COMMAND ----------

sales_df2 = spark.read.format('csv') \
  .option("inferSchema", 'true') \
  .option("header", 'true') \
  .option("sep", ',') \
  .load('/FileStore/tables/SALES_09_11.csv')
sales_df2 = sales_df2.filter(sales_df2.Country == 'US')

# COMMAND ----------

cols_all = sales_df2.columns
req_cols = [col for col in cols_all if 'mnf' in col]
schema = StructType([
    StructField("PRODUCT",StringType(),True),
    StructField("SALES",StringType(),True),
    StructField("Calendar_Quarter",StringType(),True)])
out_df2 = sqlContext.createDataFrame(sc.emptyRDD(), schema)
for col in req_cols:
    qtr = str(int(col[14:16])/3)[0:1]
    yr = col[17:21]
    qtr_str = yr+' Q'+qtr
    df = sales_df2.select('IntProd',col).withColumn('Calendar_Quarter',lit(qtr_str))
    out_df2 = out_df2.union(df)

# COMMAND ----------

sales_df3 = spark.read.format('csv') \
  .option("inferSchema", 'true') \
  .option("header", 'true') \
  .option("sep", ',') \
  .load('/FileStore/tables/SALES_12_18.csv')
sales_df3 = sales_df3.filter(sales_df3.Country == 'US')

# COMMAND ----------

cols_all = sales_df3.columns
req_cols = [col for col in cols_all if 'MNF' in col]
schema = StructType([
    StructField("PRODUCT",StringType(),True),
    StructField("SALES",StringType(),True),
    StructField("Calendar_Quarter",StringType(),True)])
out_df3 = sqlContext.createDataFrame(sc.emptyRDD(), schema)
for col in req_cols:
    qtr = col[8:10]
    yr = col[11:15]
    qtr_str = yr+' '+qtr
    df = sales_df3.select('Product',col).withColumn('Calendar_Quarter',lit(qtr_str))
    out_df3 = out_df3.union(df)
out_df3 = out_df3.filter(out_df3.Calendar_Quarter <= '2017 Q2')

# COMMAND ----------

sales_df4 = spark.read.format('csv') \
  .option("inferSchema", 'true') \
  .option("header", 'true') \
  .option("sep", ',') \
  .load('/FileStore/tables/MIDAS_Quarterly_03_2024.csv')
sales_df4 = sales_df4.filter(sales_df4.Country == 'US')

# COMMAND ----------

# Read quarterly sales for US
sales_df4 = sales_df4.select([f.col(col).alias(col.replace(' ', '_')) for col in sales_df4.columns])
sales_df4 = sales_df4.withColumn('Calendar_Quarter',concat(substring(sales_df4.Calendar_Quarter,4,4),lit(' '),substring(sales_df4.Calendar_Quarter,1,2)))
sales_df4 = sales_df4.withColumnRenamed('Product','PRODUCT')
out_df4 = sales_df4.withColumn('SALES', f.regexp_replace(f.col("USD_MNF"), "[^a-zA-Z0-9]", '').cast(FloatType()))
out_df4 = out_df4.select('PRODUCT','SALES','Calendar_Quarter')

# COMMAND ----------

col = out_df1.columns
out_fin = out_df1.select(col).unionAll(out_df2.select(col)).unionAll(out_df3.select(col)).unionAll(out_df4.select(col))

# COMMAND ----------

out_roll_up = out_fin.groupBy('PRODUCT','Calendar_Quarter').agg(sum_('SALES').alias('SALES'))

# COMMAND ----------

out_roll_up.createOrReplaceTempView('out_roll_up')
spark.sql('''CREATE OR REPLACE TABLE `gdsi-oncanaloglib`.SALES_DF AS SELECT * FROM out_roll_up''')

# COMMAND ----------

dbutils.fs.rm("/FileStore/tables/EPI_REF.csv")

# COMMAND ----------

#NDC codes in scope
ndc_df = spark.read.format('csv') \
  .option("inferSchema", 'true') \
  .option("header", 'true') \
  .option("sep", ',') \
  .load('/FileStore/tables/NDC_REF.csv')
ndc_df.createOrReplaceTempView('ndc_df')
spark.sql("CREATE OR REPLACE TABLE `gdsi-oncanaloglib`.NDC_REF as select * from ndc_df ")

# COMMAND ----------

# Procedure codes in scope
proc_cd_df = spark.read.format('csv') \
  .option("inferSchema", 'true') \
  .option("header", 'true') \
  .option("sep", ',') \
  .load('/FileStore/tables/HCPCS_REF.csv')
proc_cd_df.createOrReplaceTempView('proc_cd_df')
spark.sql("CREATE OR REPLACE TABLE `gdsi-oncanaloglib`.HCPCS_REF as select * from proc_cd_df ")

# COMMAND ----------

# Diagnosis codes in scope
diag_cd_df = spark.read.format('csv') \
  .option("inferSchema", 'true') \
  .option("header", 'true') \
  .option("sep", ',') \
  .load('/FileStore/tables/ICD_REF.csv')
diag_cd_df.createOrReplaceTempView('diag_cd_df')
spark.sql("CREATE OR REPLACE TABLE `gdsi-oncanaloglib`.ICD_REF as select * from diag_cd_df ")

# COMMAND ----------


# WS1 ref file
ws1_df = spark.read.format('csv') \
  .option("inferSchema", 'true') \
  .option("header", 'true') \
  .option("sep", ',') \
  .load('/FileStore/tables/ws1_ref_v4.csv')
ws1_df.createOrReplaceTempView('ws1_df')
spark.sql("CREATE OR REPLACE TABLE `gdsi-oncanaloglib`.WS1_REF_V2 as select * from ws1_df ")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE `gdsi-oncanaloglib`.DOS_REF AS
# MAGIC SELECT DISTINCT lower(Product_Name) AS Product_Name,days_supply,Grace_Period FROM
# MAGIC ((SELECT DISTINCT level4 AS Product_Name, 30 AS days_supply, 30 AS Grace_Period FROM us_comm_stg_prod.stg_hv_ref_ndc)
# MAGIC UNION ALL
# MAGIC (SELECT DISTINCT level3 AS Product_Name, 30 AS days_supply, 30 AS Grace_Period FROM us_comm_stg_prod.stg_hv_ref_ndc))
