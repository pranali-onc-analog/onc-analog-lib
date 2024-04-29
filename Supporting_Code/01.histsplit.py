# Databricks notebook source
database_name = 'gdsi-oncanaloglib'
analog_repo_path = '/FileStore/tables/ws1_ref_new_v2.csv'
start_date_historicalsplit = "1998-01-01"
end_date_historicalsplit = "2015-01-01"
Hisotrical_sales_data_path= '/FileStore/tables/MIDAS_Quarterly_03_2024.csv'
incidence_data_path= '/FileStore/tables/incidence_latest_data.csv'   
uptake_curves_path= '/FileStore/tables/uptake_curves_hardcoded.csv'

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Define market share and Uptake for newly launched products efficacy compared to exisitng products in reference table peak_share_reference

# COMMAND ----------

peak_share_reference = [("Better", 0.4, 7, 2),("Comparable",0.25 , 5, 3),("Lower", 0.10, 4,4),("-", 0.25, 5, 3)]
peak_share_df = spark.createDataFrame(peak_share_reference, ["Efficacy_vs_PreviousEntrant", "MarketShare_ref", "Uptake_curve_ref", "Time_to_peak_ref"])

# COMMAND ----------

from pyspark.sql.functions import lit,countDistinct,col,regexp_replace,row_number,coalesce,date_add,lag,lead,when,datediff,avg,min as min_,max as max_,sequence,explode,collect_set,array_distinct,sum as sum_, collect_list, flatten,array,array_compact,count,sort_array,expr
from pyspark.sql.types import DateType
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import lit
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, round
from pyspark.sql.functions import col, lit, round, sum as sql_sum
from pyspark.sql.window import Window
import sys
from datetime import datetime
from pyspark.sql.functions import lit,countDistinct,col,regexp_replace,row_number,coalesce,date_add,lag,lead,when,datediff,avg,min as min_,max as max_,sequence,explode,collect_set,array_distinct,sum as sum_, collect_list, flatten,array,array_compact,count,sort_array,expr
from datetime import datetime, timedelta
import calendar
import math
import sys
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DateType, FloatType, IntegerType
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, lit, round, sum as sql_sum, udf, when, 
    monotonically_increasing_id, row_number, coalesce, 
    min, max, last)
from pyspark.sql.types import StringType


# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions","auto")
spark.conf.set("spark.databricks.io.cache.enabled", "true")
sqlContext.clearCache()
spark.catalog.clearCache()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Read Analog repository excel file

# COMMAND ----------

ws1_data = spark.read.format('csv') \
  .option("inferSchema", 'true') \
  .option("header", 'true') \
  .option("sep", ',') \
  .load(analog_repo_path)
ws1_data=ws1_data.withColumnRenamed("Approval date", "approval_date") 
ws1_data.createOrReplaceTempView('ws1_data')
ws1_data.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Read Incidence excel file with annual incidence of each analog at Indication + biomarker level 

# COMMAND ----------

incidence_data = spark.read.format('csv') \
  .option("inferSchema", 'true') \
  .option("header", 'true') \
  .option("sep", ',') \
  .load(incidence_data_path)
incidence_data.createOrReplaceTempView('incidence_data')
incidence_data = incidence_data.select("TumorGroup",  "Indication","Line_of_therapy_approved",  "Patient_Segment", "Incidence")
incidence_data = incidence_data.dropDuplicates(['TumorGroup', 'Indication','Line_of_therapy_approved', 'Patient_Segment','Incidence'])
incidence_data.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Map the incidence rates for each analog  by joining with incidence_data file
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, lower

ws1_data = ws1_data.join(
    incidence_data,
    (lower(ws1_data["TumorGroup"]) == lower(incidence_data["TumorGroup"])) &
    (lower(ws1_data["Indication"]) == lower(incidence_data["Indication"])) &
    (lower(ws1_data["Line_of_therapy_approved"]) == lower(incidence_data["Line_of_therapy_approved"])) &
    (lower(ws1_data["Patient_Segment"]) == lower(incidence_data["Patient_Segment"])),
    "left").select(    ws1_data["*"],    incidence_data["Incidence"])
ws1_data = ws1_data.withColumn("PerPatientSegment_EpiPrevalence", ws1_data["Incidence"])
ws1_data=ws1_data.drop("Incidence")
ws1_data.count()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType, LongType
Schema = StructType([
    StructField('Efficacy_vs_PreviousEntrant', StringType(), True),
    StructField('AnalogBrandName', StringType(), True),
    StructField('AnalogMoleculeName', StringType(), True),
    StructField('Drug1', StringType(), True),
    StructField('Drug2', StringType(), True),
    StructField('Drug3', StringType(), True),
    StructField('Drug4', StringType(), True),
    StructField('Drug5', StringType(), True),
    StructField('TumorGroup', StringType(), True),
    StructField('Indication', StringType(), True),
    StructField('TumorType', StringType(), True),
    StructField('Line_of_therapy_approved', StringType(), True),
    StructField('Patient_Segment', StringType(), True),
    StructField('PerPatientSegment_EpiPrevalence', DoubleType(), True),
    StructField('Company', StringType(), True),
    StructField('MonoDoubletTriplet', StringType(), True),
    StructField('Class_of_MoA', StringType(), True),
    StructField('BiomarkerTestRequired', StringType(), True),
    StructField('Order_of_entry', IntegerType(), True),
    StructField('Patient_Segment_epi_prevalence', DoubleType(), True),
    StructField('approval_date', DateType(), True),
    StructField('epi_proportion', DoubleType(), True),
    StructField('MarketShare', DoubleType(), True),
    StructField('Uptake_curve', LongType(), True),
    StructField('Time_to_peak', LongType(), True),
    StructField('Date', DateType(), True)
])

## create blank df with compatible column types
temp = spark.createDataFrame([], schema=Schema)
temp.createOrReplaceTempView('temp')
query = '''CREATE OR REPLACE TABLE `{}`.main_df_test AS SELECT * FROM temp where 1==0'''.format(database_name)
spark.sql(query)


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Sorting analogs as per Approval Date and creating a list of all distinct approval date within the date range for which we want to run the historical split logic ( 1998 to 2015)

# COMMAND ----------

sorted_df = ws1_data.orderBy(col("approval_date"))
sorted_df.createOrReplaceTempView('sorted_df')
window = Window.partitionBy("TumorGroup", "Indication", "Line_of_therapy_approved", "Patient_Segment").orderBy(col("approval_date").desc())
start_date = datetime.strptime(start_date_historicalsplit, "%Y-%m-%d").date()  
end_date = datetime.strptime(end_date_historicalsplit, "%Y-%m-%d").date()
distinct_approval_dates = [
    row.approval_date 
    for row in spark.sql("SELECT DISTINCT approval_date FROM sorted_df ORDER BY approval_date").collect()
    if start_date <= row.approval_date <= end_date
]
earliest_approval_date = distinct_approval_dates[0] 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Implement following logic to create a file documenting all changing market dynamics over the years:
# MAGIC •	Iterate over each product approval date to capture market changes as new products are launched.    
# MAGIC •	On each approval date, identify all distinct products that have been launched  
# MAGIC •	For each distinct product identified in the previous step, run a subsequent loop to delve deeper into the product's specific details and market implications.  
# MAGIC •	Filter for all indications for which the product is approved at that time. Create a product_data table to handle these details.  
# MAGIC •	Calculate new epidemiological (epi) proportions for each indication based on the newly approved indications and its incidence rate.  
# MAGIC •	Update market share for the new product and all existing products in the market for the same indication, LOT, and patient segment.  
# MAGIC •	Assign 100% market share if the new product faces no competition. Otherwise, allocate the new product's market share based on the  peak_share_reference table and adjust remaining market among existing products proportionally to their previous shares. 
# MAGIC •	After processing each approval date, update the main_df table with new entries reflecting the changes. Each entry should include the 'Date' column to indicate when the market share or epidemiological proportion is modified.   
# MAGIC
# MAGIC
# MAGIC Note: for the loops to run successfully given i4iX4 large cluster size, the table `gdsi-oncanaloglib`.main_df_test  is updated at the end of every distinct approval date loop execution and the memory is cleared. 

# COMMAND ----------


for current_approval_date in distinct_approval_dates: 
    
    filtered_dataset = sorted_df.filter(sorted_df["approval_date"] == current_approval_date)
    distinct_products = filtered_dataset.select("AnalogBrandName").distinct().collect()
    query1 = ''' select * from  `{}`.main_df_test'''.format(database_name)
    main_df = spark.sql(query1)
    main_df.createOrReplaceTempView('main_df')
    iteration_count = 0
    
    for product_row in distinct_products:
         

        product = product_row["AnalogBrandName"]
        product_data = sorted_df.filter((col("AnalogBrandName") == product) & (col("approval_date") <= current_approval_date))
        total_value = product_data.select(sql_sum("PerPatientSegment_EpiPrevalence")).collect()[0][0]                    
        # Calculate new_epi_proportion
        product_data = product_data.withColumn("new_epi_proportion", col("PerPatientSegment_EpiPrevalence") / total_value)
        
        # Get rows with latest market share data from main_df and latest epi proportions from product_df
        window1 = Window.partitionBy("TumorGroup", "Indication", "Line_of_therapy_approved", "Patient_Segment").orderBy(col("Date").desc())
        
        latest_product_maindf = (main_df
                                 .filter((col("AnalogBrandName") == product) & (col("approval_date") <         current_approval_date))
                                 .withColumn("rn", row_number().over(window1))
                                 .filter(col("rn") == 1)
                                 .drop("rn"))
        
        latest_product_maindf = latest_product_maindf.join(product_data.select("TumorGroup", "Indication", "Line_of_therapy_approved", "Patient_Segment", "new_epi_proportion"),on=["TumorGroup", "Indication", "Line_of_therapy_approved", "Patient_Segment"],how="left")
        latest_product_maindf = latest_product_maindf.withColumn("epi_proportion", latest_product_maindf["new_epi_proportion"]).drop("new_epi_proportion")

        # Narrow down to indication that launched on this day to update market share of new and previously launched products
        product_data_current_new = product_data.filter(col("approval_date") == current_approval_date)
        
        dedupe_columns = ["Indication", "Line_of_therapy_approved", "Patient_Segment", "AnalogBrandName"]
        product_data_current_loop = product_data_current_new.select(dedupe_columns).dropDuplicates(dedupe_columns)   

        for row in product_data_current_loop.collect():
            product_data_current = product_data_current_new.filter((col("Indication") == row['Indication'])  & (col("Line_of_therapy_approved") == row['Line_of_therapy_approved']) &(col("Patient_Segment") == row['Patient_Segment']) &(col("approval_date") == current_approval_date))
            # Getting latest data for all other previously launched products with the same indication
            window2 = Window.partitionBy("AnalogBrandName", "indication", "Line_of_therapy_approved", "Patient_Segment").orderBy(col("Date").desc())            
            indication_data = (main_df.filter((col("indication") == row['Indication']) &(col("Line_of_therapy_approved") ==  row['Line_of_therapy_approved']) &(col("Patient_Segment") == row['Patient_Segment']) &(col("Date") < current_approval_date)).withColumn("rn", row_number().over(window2)).filter(col("rn") == 1).drop("rn"))

            if indication_data.count() == 0:
                # 100% market share if no previously launched products
                product_data_current = product_data_current.withColumn("MarketShare", lit(1)).withColumn("Uptake_curve", lit(1)).withColumn("Time_to_peak", lit(1)).withColumnRenamed("new_epi_proportion", "epi_proportion").withColumn("Date", lit(current_approval_date))
            else:
                #Market share for the new entrant
                product_data_current = product_data_current.join(peak_share_df, on="Efficacy_vs_PreviousEntrant", how="left") 
                product_data_current = (product_data_current.withColumn("MarketShare", col("MarketShare_ref")).withColumn("Uptake_curve", col("Uptake_curve_ref")).withColumn("Time_to_peak", col("Time_to_peak_ref")).drop("MarketShare_ref", "Uptake_curve_ref", "Time_to_peak_ref")).withColumnRenamed("new_epi_proportion", "epi_proportion")
                # Divide remaining market among previous entrants
                total_indication_market_share = indication_data.agg(sql_sum("MarketShare")).collect()[0][0]
                remaining_market_share = 1 - product_data_current.select("MarketShare").collect()[0][0]
                indication_data = indication_data.withColumn("Proportion", col("MarketShare") / total_indication_market_share)
                indication_data = indication_data.withColumn("MarketShare",remaining_market_share * col("Proportion")).drop("Proportion")
                # All previous entrants and new product combined into one df with latest shares            
                product_data_current = product_data_current.union(indication_data.select(*product_data_current.columns)).withColumnRenamed("new_epi_proportion", "epi_proportion").withColumn("Date", lit(current_approval_date))
            latest_product_maindf=latest_product_maindf.drop("MarketShare_ref", "Uptake_curve_ref", "Time_to_peak_ref").withColumnRenamed("new_epi_proportion", "epi_proportion")        
            
            
            if iteration_count == 0:
                
                product_union = product_data_current if latest_product_maindf.rdd.isEmpty() else product_data_current.union(latest_product_maindf.select(*product_data_current.columns))
                
                iteration_count += 1
            else:
                product_data_current=product_data_current.withColumnRenamed("new_epi_proportion", "epi_proportion")
                product_union = product_union.withColumnRenamed("new_epi_proportion", "epi_proportion")
            
                product_union = (product_union.select(*product_data_current.columns).union(product_data_current).union(latest_product_maindf.select(*product_data_current.columns)))
        product_union = product_union.withColumn("Date", lit(current_approval_date))
    
    product_union=product_union.drop("MarketShare_ref", "Uptake_curve_ref", "Time_to_peak_ref").withColumnRenamed("new_epi_proportion", "epi_proportion")        
    main_df=main_df.drop("MarketShare_ref", "Uptake_curve_ref", "Time_to_peak_ref")
    if current_approval_date == earliest_approval_date:
        main_df = product_union
        
    else:
        column_sequence = product_union.columns 
        main_df = (main_df.select(*column_sequence)
           .union(product_union.select(*column_sequence)))
            
        
    main_df.createOrReplaceTempView('main_df')
    
    query2 = ''' CREATE OR REPLACE TABLE `{}`.main_df_test as (SELECT * from main_df) '''.format(database_name)
    spark.sql(query2)
    def clear_resources(view_name, dataframe):
        spark.catalog.dropTempView(view_name)
        if dataframe is not None:
            dataframe.unpersist()

    resource_list = [
        ("filtered_dataset", filtered_dataset),
        ("main_df", main_df),
        ("product_data", product_data),
        ("latest_product_maindf", latest_product_maindf),        
        ("product_data_current", product_data_current),
        ("indication_data", indication_data)
    ]

    for view_name, dataframe in resource_list:
        clear_resources(view_name, dataframe)
    sqlContext.clearCache()
    spark.catalog.clearCache()        

# COMMAND ----------

query3 = ''' SELECT * FROM `{}`.main_df_test '''.format(database_name)
product_data=spark.sql(query3)
product_data.createOrReplaceTempView('product_data')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Define Uptake curve function  
# MAGIC Retrieve growth characteristics from 10 pre-defined uptake curves stored in 'uptake_curves_path' and scaling them to their respective timespans, start, and end points.The function takes the current_month, start_month, end_month, uptake_curve_number, peak_market_share, previous_month_market_share,previous_transition_share as inputs and returns the current months market share.

# COMMAND ----------

import pandas as pd
import numpy as np

uptake_ref = spark.read.format('csv') \
  .option("inferSchema", 'true') \
  .option("header", 'true') \
  .option("sep", ',') \
  .load(uptake_curves_path)

uptake_ref=uptake_ref.toPandas()

uptake_ref.set_index('Uptake curve ', inplace=True)
uptake_ref_pivot = uptake_ref.T

def calculate_current_share_udf(current_month, start_month, end_month, uptake_curve_number, peak_market_share, previous_month_market_share,previous_transition_share):
    if None in [current_month, start_month, end_month, uptake_curve_number, peak_market_share, previous_month_market_share]:
        return 0.0
    if previous_transition_share is None:
        previous_transition_share = 0

    current_month = int(current_month)
    start_month = int(start_month)
    end_month = int(end_month)
    uptake_curve_number = int(uptake_curve_number)
    peak_market_share = float(peak_market_share)
    previous_month_market_share = float(previous_month_market_share)
    previous_transition_share = float(previous_transition_share)
    # Validate the current month is within the range
    if current_month < start_month or current_month > end_month:
        return float(previous_month_market_share)
    else:
        # Fetch the reference uptake curve and calculate number of months
        ref_curve = uptake_ref_pivot[uptake_curve_number]
        total_months = end_month - start_month
        months_from_start = current_month - start_month
        
        # Scale the uptake curve from the reference to match the desired transition period
        scaled_curve = np.interp(np.arange(total_months + 1),
                                 np.linspace(0, total_months, len(ref_curve)),
                                 ref_curve)
        max_value = scaled_curve.max()
        # Reverse uptake curve to a downward sloping curve for reduction in market share
        if previous_transition_share > peak_market_share:
            # Adjust the curve to start from the previous month's market share to new peak market share
            scaled_curve = np.max(scaled_curve) - scaled_curve  # reverse curve
            min_value = peak_market_share
            max_value = previous_month_market_share

        else:
            # Upward curve logic
            min_value = previous_month_market_share
            max_value = peak_market_share
        scaled_curve = np.interp(
            scaled_curve,
            (scaled_curve.min(), scaled_curve.max()),
            (min_value, max_value)
        )    
           
        # Calculate current share based on the adjusted curve
        current_share = np.interp(
            months_from_start,
            np.arange(total_months + 1),
            scaled_curve
        )
        
        return float(current_share)

calculate_current_share = udf(calculate_current_share_udf, FloatType())


# COMMAND ----------

# MAGIC %md
# MAGIC ##### 
# MAGIC •	Generate columns representing each month over the entire timeline of the dataset for analyzing temporal changes in market share and other metrics on a month-to-month basis.  
# MAGIC •	Partition the data for each unique combination of Product, Indication, Line of Therapy (LOT), and Biomarker. Order each partition by the 'Date' column.  
# MAGIC •	Within each partition, assign a row number to each record in the assigned order representing the sequence of changes within each partition.
# MAGIC
# MAGIC

# COMMAND ----------

df = product_data

windowSpec = Window.partitionBy('AnalogBrandName','Indication', 'Line_of_therapy_approved', 'Patient_Segment').orderBy('Date')
df = df.withColumn("row_id", row_number().over(windowSpec))

# Convert 'Time_to_peak' years to months 
df = df.withColumn("Time_to_peak_months", col("Time_to_peak") * 12)

df = df.withColumn("Time_to_peak_months", col("Time_to_peak_months").cast(FloatType()))
df = df.withColumn("MarketShare", col("MarketShare").cast(FloatType()))

min_date, max_date = df.select(min("Date"), max("Date")).first()


# list of months between the minimum and maximum dates
month_list = [min_date + timedelta(days=i) for i in range((max_date - min_date).days + 1) if (min_date + timedelta(days=i)).day == 1]


def month_diff(date):
    return (date.year - min_date.year) * 12 + date.month - min_date.month

month_diff_udf = udf(month_diff, IntegerType())

df = df.withColumn("month_tag", month_diff_udf(col("Date")))
df = df.withColumn("Month_at_peak", col("Time_to_peak_months") + col("month_tag"))

# months to generate columns for
num_months = len(month_list)

for i in range(num_months):
    month_col_name = f"Market_Share_Month_{i+1}"
    df = df.withColumn(month_col_name, lit(None))
df = df.withColumn('MarketShare', col('MarketShare') * 100)


# COMMAND ----------

partitionWindow = Window.partitionBy('AnalogBrandName' ,'Indication', 'Line_of_therapy_approved', 'Patient_Segment').orderBy('row_id')

df = df.withColumn("prev_marketshare_temp", lag("marketshare").over(windowSpec))
df = df.withColumn("prev_month_tag_temp", lag("Month_at_peak").over(windowSpec))
df = df.withColumn("prev_startmonth_tag_temp", lag("month_tag").over(windowSpec))
from pyspark.sql.functions import col, when, coalesce

df = df.withColumn("Month_at_peak", 
                   when(col("marketshare") == col("prev_marketshare_temp"), col("prev_month_tag_temp"))
                   .otherwise(col("Month_at_peak")))


df = df.withColumn("start_month_reference", 
                   when(col("marketshare") == col("prev_marketshare_temp"), coalesce(col("prev_startmonth_tag_temp"), col("month_tag")))
                   .otherwise(col("month_tag")))


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Calculate market share transtion for all products based on mapping created earlier
# MAGIC • For each subgroup, process the data row by row. Row 1 represents the initial assignment of market share and  subsequent rows (Row 2, Row 3, etc.), represent any changes to market share.  
# MAGIC •	Apply the uptake function to each row to model the transition of market share and assign market share values for each month by calculating the expected share for that month.  
# MAGIC •	After processing each row, update the `gdsi-oncanaloglib`.uptake_df_final table with the latest calculated data to ensure all changes are recorded.  
# MAGIC •	Clear memory after each update to optimize system performance and prevent data processing slowdowns. 
# MAGIC

# COMMAND ----------

def get_previous_month_index(month_tag):
    return month_tag - 2 if month_tag and month_tag > 1 else None
def get_value_from_array(arr, index):
    return arr[index] if arr is not None and index is not None and index < len(arr) else None
get_previous_month_index_udf = udf(get_previous_month_index, IntegerType())
get_value_from_array_udf = udf(get_value_from_array, StringType())

max_row_id = df.agg({"row_id": "max"}).collect()[0][0]

for row in range(1, max_row_id + 1):
    if row>1:
        query4 = ''' select * from  `{}`.uptake_df_final '''.format(database_name)
        df = spark.sql(query4)
        
    
    df = df.withColumn('prev_month_index', get_previous_month_index_udf(col('month_tag')))
    market_share_cols = [F.col(f'Market_Share_Month_{i}') for i in range(1, num_months + 1)]  
    df = df.withColumn('market_share_values', F.array([coalesce(c, lit(0)) for c in market_share_cols]))
    partitionWindow = Window.partitionBy('AnalogBrandName', 'Indication', 'Line_of_therapy_approved', 'Patient_Segment').orderBy('row_id')
    df = df.withColumn('prev_market_share_values', F.lag('market_share_values', 1).over(partitionWindow))
    df = df.withColumn('prev_month_market_share', get_value_from_array_udf('prev_market_share_values', 'prev_month_index'))


    for month in range(1, num_months + 1):
        current_month_col = f"Market_Share_Month_{month}"
        previous_month_col = f"Market_Share_Month_{month - 1}" if month > 1 else None
        if row == 1:
            
            if previous_month_col:
                
                update_condition = (
                    when((col("row_id") == row) & (col("month_tag") == month), lit(0))
                    .when((col("row_id") == row) & (col("month_tag") < month),
                          calculate_current_share(lit(month), col("start_month_reference"), col("Month_at_peak"), col("Uptake_curve"), col("MarketShare"), col(previous_month_col),col("prev_marketshare_temp"))).otherwise(lit(0))
                )
            
            else:
                update_condition = lit(0) 
            
                
            df = df.withColumn(current_month_col, update_condition)
        else:
            if previous_month_col:
                update_condition = (
                when(col("row_id") == row, 
                     when(col("month_tag") == month, col('prev_month_market_share'))
                     .when(col("month_tag") < month,
                           calculate_current_share(lit(month), col("start_month_reference"), col("Month_at_peak"),col("Uptake_curve"), col("MarketShare"),col(previous_month_col), col("prev_marketshare_temp"))))
                .otherwise(col(current_month_col))  
            )      
            else:
                update_condition = when(col("row_id") == row, lit(0)).otherwise(col(current_month_col))      
            df = df.withColumn(current_month_col, update_condition)
    df.createOrReplaceTempView('df')
    # df.printSchema()
    query6 = ''' CREATE OR REPLACE TABLE `{}`.uptake_df_final as (SELECT * from df) '''.format(database_name)
    spark.sql(query6)
    
    spark.catalog.dropTempView('df')
    df.unpersist()
    sqlContext.clearCache()
    spark.catalog.clearCache()        
 

# COMMAND ----------

query7 = ''' SELECT * FROM `{}`.uptake_df_final '''.format(database_name)
dff = spark.sql(query7)
dff.createOrReplaceTempView('dff')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Multiply the market shares with EPI % associated with each indication to get the Final split within a product based on incidence of the tumor in the population and the effectiveness of that product to treat that tumor type 

# COMMAND ----------

# Multiply with Epi
from pyspark.sql.functions import col

market_share_columns = [c for c in dff.columns if c.startswith('Market_Share_Month')]
column_expressions = [
    (col(column) * col('epi_proportion')).alias(column) if column.startswith('Market_Share_Month') 
    else col(column)
    for column in dff.columns
]
dff = dff.select(column_expressions)



# COMMAND ----------

# MAGIC %md
# MAGIC Add a column to denote the month number when the transition to the new market share from the current market share begins.

# COMMAND ----------

partitionWindow = Window.partitionBy('AnalogBrandName', 'AnalogMoleculeName' ,'Indication', 'Line_of_therapy_approved', 'Patient_Segment').orderBy('Date')
dff = dff.withColumn('Next_month_tag', lead('month_tag').over(partitionWindow))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Consolidate the market share transitions for each Product+Indication+LOT+Biomarker subgroup into a single market share row 
# MAGIC - Pivot the dataset 
# MAGIC - Filter for rows where current date is > the 'Date' from which the transition is supposed to start and less than 'Next month tag' created  
# MAGIC - Reverse the month numbers to actual dates using reverse_month_diff_udf function.

# COMMAND ----------


##Pivot dataframe
market_share_columns = [col for col in dff.columns if col.startswith("Market_Share_Month_")]

# Get all columns except market share columns
all_columns_except_months = [col for col in dff.columns if col not in market_share_columns]

dff.createOrReplaceTempView('dff')
dff = dff.selectExpr(*all_columns_except_months,"stack({}, {}) as (Market_share_months, Share)".format(len(market_share_columns), ", ".join(["'{}', {}".format(col, col) for col in market_share_columns])))


from pyspark.sql.functions import regexp_extract, col
df1 = dff.withColumn("month_number", regexp_extract(col("Market_share_months"), r'\d+$', 0))

from pyspark.sql.functions import max, lit, coalesce

df1 = df1.withColumn('month_number', df1['month_number'].cast('int'))
max_value_df = df1.agg(max('month_number').alias('MaxValue'))
max_value = max_value_df.collect()[0]['MaxValue']
df1 = df1.withColumn('Next_month_tag', coalesce(dff['Next_month_tag'], lit(max_value)))
df1.createOrReplaceTempView('df1')
df1 = spark.sql(''' SELECT * FROM df1 where month_number > month_tag and month_number <= Next_month_tag ''')
df1.createOrReplaceTempView('df1')

start_date = df1.agg(min("Date")).collect()[0][0]
df1 = df1.withColumn("start_date", lit(start_date))
df1 = df1.withColumn("month_number", col("month_number").cast("int"))
 
@udf(DateType())
def reverse_month_diff_udf(month_number, start_date):
    years_diff = month_number // 12
    remaining_months = month_number % 12
    year = start_date.year + years_diff
    month = start_date.month + remaining_months
    if month > 12:
        year += 1
        month -= 12
    return datetime(year, month, start_date.day)


df1 = df1.withColumn("Date_current1", reverse_month_diff_udf(df1["month_number"], df1["start_date"]))

df1.createOrReplaceTempView('df1')


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create a Quarter+Year column using the Date column

# COMMAND ----------

from pyspark.sql.functions import year, quarter, format_string
df1 = df1.withColumn("Date_current1", df1["Date_current1"].cast('date'))
df1 = df1.withColumn("year", year("Date_current1")) \
         .withColumn("Qtr", quarter("Date_current1"))
df1 = df1.withColumn("Quarter", format_string("Q%d %d", df1["Qtr"], df1["year"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### For every distinct Product+Regimen+Indication+LOT+Biomarker subgroup , group at quarter level and take the sum of final share to calculate final sales allocation proportions 
# MAGIC - Calculate Final Allocation of sales amongst all indications approved for a product at a given point in time by calcualting the proportion within each quarters final share total

# COMMAND ----------

grouping_columns = [
    'AnalogBrandName', 'AnalogMoleculeName', 'MonoDoubletTriplet','Indication', 
    'Line_of_therapy_approved', 'Patient_Segment', 'Quarter'
]

aggregated_df = df1.groupBy(grouping_columns).agg(sum_('Share').alias('Total_Share'))
## FINAL ALLOCATION
from pyspark.sql.functions import col, sum as sum_
from pyspark.sql.window import Window
grouping_columns = ['AnalogBrandName', 'Quarter']
window_spec = Window.partitionBy(grouping_columns)
sum_column = sum_(col('Total_Share')).over(window_spec)
aggregated_df = aggregated_df.withColumn('Final_allocation', col('Total_Share') / sum_column)


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Load US sales data

# COMMAND ----------

query_salesdf = ''' SELECT * FROM `{}`.SALES_DF '''.format(database_name)
sales_df=spark.sql(query_salesdf)
sales_df.createOrReplaceTempView('sales_df')

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import FloatType

qtr_sales = sales_df.groupBy('Calendar_Quarter','PRODUCT').agg(f.sum('SALES').alias('TOT_SALES'))

qtr_sales_dummy = qtr_sales.withColumn('ANALOG',f.lower(qtr_sales.PRODUCT))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Left join quarterly sales data at Product level and multiply with Final allocation to get Final Output saved in `gdsi-oncanaloglib`.Hist_sales_final_output
# MAGIC

# COMMAND ----------

qtr_sales_dummy = qtr_sales_dummy.withColumn(
    'adjusted_date', 
    f.concat(f.split(qtr_sales_dummy['Calendar_Quarter'], ' ')[1], f.lit(' '), f.split(qtr_sales_dummy['Calendar_Quarter'], ' ')[0])
)


# COMMAND ----------

aggregated_df = aggregated_df.withColumn('ANALOG',f.lower(aggregated_df.AnalogBrandName))
qtr_sales_dummy = qtr_sales_dummy.select("ANALOG", "adjusted_date", "TOT_SALES")
Joined_sales = aggregated_df.join(qtr_sales_dummy, (aggregated_df.ANALOG == qtr_sales_dummy.ANALOG) & (aggregated_df.Quarter == qtr_sales_dummy.adjusted_date), "left")

# COMMAND ----------



Final_output = Joined_sales.withColumn("Sales_final", col("Final_allocation") * col("TOT_SALES"))
Final_output=Final_output.select('AnalogBrandName',
 'AnalogMoleculeName',
 'MonoDoubletTriplet',
 'Indication',
 'Line_of_therapy_approved',
 'Patient_Segment',
 'Quarter', 
 'Sales_final')
Final_output.createOrReplaceTempView('Final_output')


# COMMAND ----------

query8 = ''' CREATE OR REPLACE TABLE `{}`.Hist_sales_final_output as (SELECT * from Final_output) '''.format(database_name)
spark.sql(query8)
