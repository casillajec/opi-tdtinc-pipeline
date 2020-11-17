from datetime import date
import os
from os.path import abspath

from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, concat_ws, date_format, lag, month,
                                   sum, to_timestamp, year)
from pyspark.sql.window import Window

spark = SparkSession.builder.appName('ERP_Preprocessing').getOrCreate()

# Day of Execution, from Airflow
ds_string = os.getenv('AIRFLOW_DS', date.today().strftime('%Y%m%d'))

# Extract
files = abspath(f'datalake/crudo/generador/fuente/{ds_string}/erp/*.csv')
df = (spark
    .read.csv(files)
    .withColumnRenamed('_c0', 'year')
    .withColumnRenamed('_c1', 'month')
    .withColumnRenamed('_c2', 'city')
    .withColumnRenamed('_c3', 'type')
    .withColumnRenamed('_c4', 'category')
    .withColumnRenamed('_c5', 'region')
    .withColumnRenamed('_c6', 'somecode')
    .withColumnRenamed('_c7', 'product')
    .withColumnRenamed('_c8', 'total_spend'))

# Transform
df = (df
    .withColumn('ym', concat_ws('-', col('year'), col('month')))
    .withColumn('ym', to_timestamp('ym', 'yyyy-MMM'))
    .withColumn('total_spend', df['total_spend'].cast('float')))

# (a)
monthly_df = (df
    .groupBy('region', 'ym')
    .agg(sum('total_spend').alias('monthly_spend'))
    .cache())

# (b)
w_spec_ytd = Window().partitionBy('region', year('ym')).orderBy(month('ym'))
ytd_df = (monthly_df
    .withColumn('ytd', sum(col('monthly_spend')).over(w_spec_ytd))
    .drop('monthly_spend'))

# (c)
w_spec_pdif = Window().partitionBy('region').orderBy(year('ym'), month('ym'))
pdif_df = (monthly_df
    .withColumn('pmonth', lag('monthly_spend').over(w_spec_pdif))
    .withColumn('pdif', (col('monthly_spend')/col('pmonth'))*100)
    .drop('pmonth', 'monthly_spend'))

final_df = (monthly_df
    .join(ytd_df, on=['region', 'ym'])
    .join(pdif_df, on=['region', 'ym'])
    .withColumn('year', year('ym'))
    .withColumn('month', date_format(col('ym'), 'MMM'))
    .select('region', 'year', 'month', 'monthly_spend', 'pdif', 'ytd')
    .cache())

# I'm assuming all regions fit into the driver's RAM
regs = [x[0] for x in final_df.select('region').distinct().collect()]
groups = [(reg, final_df.filter(col('region') == reg)) for reg in regs]

# Load
for reg, group in groups:
    path = abspath(f'datalake/procesado/generador/fuente/{ds_string}/erp/{reg}')  # I tried
    group.coalesce(1).write.csv(path)


