from datetime import date
from itertools import product
import os
from os.path import abspath

from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, concat_ws, date_format, lag, month,
                                   sum, to_timestamp, year)
from pyspark.sql.window import Window

spark = SparkSession.builder.appName('TI_Preprocessing').getOrCreate()

# Day of Execution, from Airflow
ds_string = os.getenv('AIRFLOW_DS', date.today().strftime('%Y%m%d'))

# Extract
fact_table_path = abspath(f'datalake/crudo/generador/fuente/{ds_string}/teinvento/fact_table')
fact_table_df = (spark
    .read.csv(fact_table_path)
    .withColumnRenamed('_c0', 'year')
    .withColumnRenamed('_c1', 'month')
    .withColumnRenamed('_c2', 'total_spend')
    .withColumnRenamed('_c3', 'region_code')
    .withColumnRenamed('_c4', 'product_code'))

product_dim_path = abspath(f'datalake/crudo/generador/fuente/{ds_string}/teinvento/product_dim')
product_dim_df = (spark
    .read.csv(product_dim_path)
    .withColumnRenamed('_c0', 'product_code')
    .withColumnRenamed('_c1', 'category')
    .withColumnRenamed('_c2', 'product')
    .withColumnRenamed('_c3', 'product_desc')
    .withColumnRenamed('_c4', 'company'))

region_dim_path = abspath(f'datalake/crudo/generador/fuente/{ds_string}/teinvento/region_dim')
region_dim_df = (spark
    .read.csv(region_dim_path)
    .withColumnRenamed('_c0', 'region_code')
    .withColumnRenamed('_c1', 'city')
    .withColumnRenamed('_c2', 'region'))

# Transform
df = (fact_table_df
    .join(product_dim_df, on=['product_code'])
    .join(region_dim_df, on=['region_code'])
    .drop('region_code', 'product_code'))
    
df = (df
    .withColumn('ym', concat_ws('-', col('year'), col('month')))
    .withColumn('ym', to_timestamp('ym', 'yyyy-MMM'))
    .withColumn('total_spend', df['total_spend'].cast('float')))

# (a)
monthly_df = (df
    .groupBy('company', 'region', 'ym')
    .agg(sum('total_spend').alias('monthly_spend'))
    .cache())

# (b)
w_spec_ytd = (Window()
    .partitionBy('company', 'region', year('ym'))
    .orderBy(month('ym')))
ytd_df = (monthly_df
    .withColumn('ytd', sum(col('monthly_spend')).over(w_spec_ytd))
    .drop('monthly_spend'))

# (c)
w_spec_pdif = (Window()
    .partitionBy('company', 'region')
    .orderBy(year('ym'), month('ym')))
pdif_df = (monthly_df
    .withColumn('pmonth', lag('monthly_spend').over(w_spec_pdif))
    .withColumn('pdif', (col('monthly_spend')/col('pmonth'))*100)
    .drop('pmonth', 'monthly_spend'))

final_df = (monthly_df
    .join(ytd_df, on=['company', 'region', 'ym'])
    .join(pdif_df, on=['company', 'region', 'ym'])
    .withColumn('year', year('ym'))
    .withColumn('month', date_format(col('ym'), 'MMM'))
    .select('company', 'region', 'year', 'month', 'monthly_spend', 'pdif', 'ytd')
    .cache())

# I'm assuming all regions and companies fit into the driver's RAM
comps = [x[0] for x in final_df.select('company').distinct().collect()]
regs = [x[0] for x in final_df.select('region').distinct().collect()]

groups = [(comp, reg, final_df.filter((col('company') == comp) & (col('region') == reg))) for comp, reg in product(comps, regs)]
# Load
for comp, reg, group in groups:
    path = abspath(f'datalake/procesado/generador/fuente/{ds_string}/teinvento/{comp}/{reg}')
    group.coalesce(1).write.csv(path)
