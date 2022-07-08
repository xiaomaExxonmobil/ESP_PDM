# Databricks notebook source
# MAGIC %md
# MAGIC ## Tutorial to query data from ODS, DWH, and ADX

# COMMAND ----------

# MAGIC %run "Config/Functions"

# COMMAND ----------

from datetime import date, datetime
from datetime import timedelta

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prepare folders to write data into

# COMMAND ----------

#dbutils.fs.ls('/mnt/devadlstorage01/shared/')

# COMMAND ----------

#dbutils.fs.rm('/mnt/devadlstorage01/shared/test', True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Procount Daily - Query ODS tables

# COMMAND ----------

startDate = date(2018,1,1)
#endDate = date(2022,1,30)
endDate = date(2022,1,1)

skip = 30 #days

curr = startDate
end = curr + timedelta(days = skip)
i = 0
while end < endDate:
  
  print(end)
  
  query = """
  SELECT [MerrickID]
        ,[RecordDate]
        ,[ProducingStatus]
        ,[ProducingMethod]
        ,[OilProduction]
        ,[WaterProduction]
        ,[AllocEstGasVolMCF] as GasProduction
        ,[AllocEstInjGasVolMCF] as GasInjection
        ,AllocEstInjWaterVol as WaterInjection
        ,UserNumber1 as LinePressure
        ,[CasingPressure]
        ,SurfaceCasingPressure
        ,InterCasingPressure
        ,[InjectionPressure]
        ,[TubingPressure]
        ,ChokeSize as ChokeSetting
        ,UserNumber3 AS ChokeSize
    FROM [procount].[CompletionDailyTb] WITH (NOLOCK)    
    where RecordDate >='{}' AND RecordDate <'{}'
  """.format(curr, end)
  
  df = QuerySqlDatabaseSelect('ODS',query)
  
  for column in df.columns:
    df = df.withColumn('MerrickID',col('MerrickID').cast('Integer'))
    df = df.withColumn('RecordDate',to_date(col("RecordDate"),"yyyy-MM-dd") )
    if column not in ['MerrickID','RecordDate']:
      if column in ['ProducingStatus','ProducingMethod']:
        df = df.withColumn(column,col(column).cast('Integer'))                
      else:
        df = df.withColumn(column,col(column).cast('Double'))
   
  # overwrite the first time, then append
  if i == 0:
    df.write.format('delta').mode('overwrite').save('/mnt/devadlstorage01/shared/ESP_Xiao/')     
  else:
    df.write.format('delta').mode('append').save('/mnt/devadlstorage01/shared/ESP_Xiao/')     
 
  i = i + 1
  
  curr = end
  end = end + timedelta(days=skip)
  
display(df)


# COMMAND ----------

display(df)

# COMMAND ----------

print(df.count(), len(df.columns))

# COMMAND ----------

df_well = df.filter(col('MerrickID')==26).toPandas()

# COMMAND ----------

import matplotlib.pyplot as plt
plt.plot(df_well['RecordDate'], df_well['Water'])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create table

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- create database test 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- create table test.table using delta location '/mnt/devadlstorage01/shared/test'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- optimize test.table

# COMMAND ----------

df = spark.read.format('delta').load('/mnt/devadlstorage01/shared/test')
df.count()

# COMMAND ----------

downtime = spark.sql("select * from test.table")
print(downtime.count())
print(downtime.columns)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Ignition - Query ADX tables

# COMMAND ----------

for day in range(183):
  ignition_query = """
  IgnitionWellPad
  | where RecordDateTime >= datetime('2021-01-01T00:00:00.000Z')+timespan({}d) and RecordDateTime < datetime('2021-01-01T00:00:00.000Z')+timespan({}d) and APINumber != ""
  | where Measurement in (
  "Flow_Line_1/Pressure/PV",
  "Flow_Line_1/Pressure/HiHiSP",
  "Flow_Line_1/Pressure/LoLoSP",
  "Tubing/Pressure/PV",
  "Tubing/Pressure/HiHiSP",
  "Tubing/Pressure/LoLoSP",
  "Surface_Casing/Pressure/PV",
  "Surface_Casing/Pressure/HiHiSP",
  "Surface_Casing/Pressure/LoLoSP",
  "Intermediate_Casing/Pressure/PV",
  "Intermediate_Casing/Pressure/HiHiSP",
  "Intermediate_Casing/Pressure/LoLoSP",
  "Intermediate_Casing_2/Pressure/PV",
  "Intermediate_Casing_2/Pressure/HiHiSP",
  "Intermediate_Casing_2/Pressure/LoLoSP",
  "Casing/Pressure/PV",
  "Casing/Pressure/HiHiSP",
  "Casing/Pressure/LoLoSP",
  "Gas_Lift/Gas_Meter/FlowRatePerDay",
  "Gas_Lift/Gas_Meter/PID/SP",
  "Gas_Lift/Gas_Meter/Config/StaticPressure",
  "Gas_Lift/Gas_Meter/Config/Temperature",
  "Downhole_Gauge/Gauge_1/Pressure",
  "Downhole_Gauge/Gauge_1/Temperature"
  )
  |summarize Value = round(avg(todouble(Value)),2) by APINumber, Measurement, RecordDate, bin(RecordDateTime, 30m)
  |evaluate pivot(Measurement, take_any(Value))
  """.format(day,day+1)

  adx_df = query_adx(ignition_query)
  print('Finished Reading ADX')
  adx_df = (adx_df
            .withColumnRenamed('Casing/Pressure/PV','CasingPressure')
            .withColumnRenamed('Casing/Pressure/HiHiSP','CasingPressureHiHiSP')
            .withColumnRenamed('Casing/Pressure/LoLoSP','CasingPressureLoLoSP')
            .withColumnRenamed('Tubing/Pressure/PV','TubingPressure')
            .withColumnRenamed('Tubing/Pressure/HiHiSP','TubingPressureHiHiSP')
            .withColumnRenamed('Tubing/Pressure/LoLoSP','TubingPressureLoLoSP')
            .withColumnRenamed('Flow_Line_1/Pressure/PV','LinePressure')
            .withColumnRenamed('Flow_Line_1/Pressure/HiHiSP','LinePressureHiHiSP')
            .withColumnRenamed('Flow_Line_1/Pressure/LoLoSP','LinePressureLoLoSP')
            .withColumnRenamed('Intermediate_Casing/Pressure/PV','IntermediateCasingPressure')
            .withColumnRenamed('Intermediate_Casing/Pressure/HiHiSP','IntermediateCasingPressureHiHiSP')
            .withColumnRenamed('Intermediate_Casing/Pressure/LoLoSP','IntermediateCasingPressureLoLoSP')
            .withColumnRenamed('Intermediate_Casing_2/Pressure/PV','IntermediateCasing2Pressure')
            .withColumnRenamed('Intermediate_Casing_2/Pressure/HiHiSP','IntermediateCasing2PressureHiHiSP')
            .withColumnRenamed('Intermediate_Casing_2/Pressure/LoLoSP','IntermediateCasing2PressureLoLoSP')
            .withColumnRenamed('Surface_Casing/Pressure/PV','SurfaceCasingPressure')
            .withColumnRenamed('Surface_Casing/Pressure/HiHiSP','SurfaceCasingPressureHiHiSP')
            .withColumnRenamed('Surface_Casing/Pressure/LoLoSP','SurfaceCasingPressureLoLoSP')
            .withColumnRenamed('Gas_Lift/Gas_Meter/Config/StaticPressure','GasLiftInjectionPressure')
            .withColumnRenamed('Gas_Lift/Gas_Meter/Config/Temperature','GasLiftInjectionTemperature')
            .withColumnRenamed("Gas_Lift/Gas_Meter/FlowRatePerDay",'GasLiftInjectionRate')
            .withColumnRenamed("Gas_Lift/Gas_Meter/PID/SP",'GasLiftInjectionRateSP')
            .withColumn('RecordDate',to_date(col("RecordDate"),"yyyy-MM-dd") )
            .withColumnRenamed('Downhole_Gauge/Gauge_1/Pressure','DownholePressure')
            .withColumnRenamed('Downhole_Gauge/Gauge_1/Temperature','DownholeTemperature')
            )
  #adx_df.write.format('delta').mode('append').partitionBy('RecordDate').save('/mnt/prodadlstorage03/data/delta/agame/ignition30minute5')
  print(day)
