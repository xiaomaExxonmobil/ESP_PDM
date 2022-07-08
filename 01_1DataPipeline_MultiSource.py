# Databricks notebook source
# MAGIC %md
# MAGIC ## Query data and create tables for Liquid loading problem

# COMMAND ----------

# MAGIC %md
# MAGIC %run "Config/Functions"

# COMMAND ----------

# MAGIC %run "Config/Functions"

# COMMAND ----------

#%run "/Users/amit.x.kumar@exxonmobil.com/utility/Config_Functions"

# COMMAND ----------

from datetime import date, datetime
from datetime import timedelta

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prepare folders to write data into

# COMMAND ----------

dbutils.fs.mkdirs('/mnt/devadlstorage01/shared/xiaozhang')

# COMMAND ----------

#dbutils.fs.rm('/mnt/devadlstorage01/shared/xiaozhang/liquid_loading/downtime', True)
#dbutils.fs.rm('/mnt/devadlstorage01/shared/xiaozhang/liquid_loading/PAwelldailyJoined', True)
#dbutils.fs.rm('/mnt/devadlstorage01/shared/xiaozhang/liquid_loading/well', True)
#dbutils.fs.rm('/mnt/devadlstorage01/shared/xiaozhang/liquid_loading/welltubingcomponent',True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Downtime

# COMMAND ----------

# MAGIC %md
# MAGIC Tables used are as follows: <br>
# MAGIC * procount.Completiontb c <br>
# MAGIC * procount.completiondailytb cd  <br>
# MAGIC * procount.DowntimeReasonTb dtr  <br>
# MAGIC * procount.DowntimeCodeTb dc  <br>
# MAGIC * procount.XTO_CplLostProd clp  <br>                     

# COMMAND ----------

# break the query into smaller queries to prevent timeout
startDate = date(2019,1,1)
endDate = date(2022,1,1)

val = int((endDate-startDate).total_seconds() / (60 * 60 * 24 * 30)) # convert to months

for idx in range(val):
  day_start = startDate + timedelta(days=32*idx)
  day_end = startDate + timedelta(days=32*(idx+1))
  start = datetime(day_start.year,day_start.month,1)
  end = datetime(day_end.year,day_end.month,1)
  print(start,end)

  ods_downtime_query = """
  SELECT  c.MerrickID, 
                        --c.ApiWellNumber AS Api10,
                        cd.RecordDate, 
                        DowntimeHours,
                        dc.DowntimeCode,
                        dc.DowntimeDescription AS DowntimeReasonDescription, 
                        dtr.Comments AS DowntimeComments,
                        clp.OilLostProduction,
                        clp.GasLostProduction,
                        clp.WaterLostProduction,
                        clp.CalculationType AS LostProdCalculationType
                FROM procount.Completiontb c
                        INNER JOIN procount.completiondailytb cd 
                               ON c.merrickid = cd.merrickid
                        LEFT JOIN procount.DowntimeReasonTb dtr 
                               ON dtr.OriginalDateEntered = cd.RecordDate
                               AND dtr.ObjectMerrickID = c.MerrickID
                               AND dtr.DeleteFlag = 2                            --Don't pull deleted records
                               AND dtr.ObjectType = 1                            --Only pull well downtime
                               AND dtr.DowntimeHours <> 0                 --Don't pull downtime records with 0 hours
                        LEFT JOIN procount.DowntimeCodeTb dc 
                               ON dtr.DowntimeCode = dc.DowntimeCode 
                        LEFT JOIN procount.XTO_CplLostProd clp 
                               ON clp.cmid = c.MerrickID 
                               AND clp.RecordDate = cd.RecordDate       
                WHERE dtr.DowntimeHours IS NOT NULL               --Only pull records with downtime
                         AND cd.recorddate >='{}' AND cd.recorddate <'{}'

  """.format(start, end)
  ods_downtime = (QuerySqlDatabaseSelect('ODS',ods_downtime_query)
                  .withColumn('MerrickID',col('MerrickID').cast('String'))
                  .withColumn('RecordDate',to_date(col("RecordDate"),"yyyy-MM-dd"))
                  .withColumn('DowntimeCode',col('DowntimeCode').cast('Integer'))
                 )
  
  for column in ['DowntimeHours','OilLostProduction','GasLostProduction','WaterLostProduction']:
    ods_downtime = ods_downtime.withColumn(column,col(column).cast('double'))
  for column in ['DowntimeReasonDescription','DowntimeComments','LostProdCalculationType']:
    ods_downtime = ods_downtime.withColumn(column,col(column).cast('string'))
    
  file_path = r'/mnt/devadlstorage01/shared/xiao_ma/esp_failure/downtime/'
  ods_downtime.write.format('delta').mode('append').save(file_path) 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create table

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE DATABASE IF NOT EXISTS xiaozhang 

# COMMAND ----------

# MAGIC %sql
# MAGIC create table xiaozhang.liquid_loading_downtime using delta location '/mnt/devadlstorage01/shared/xiaozhang/liquid_loading/downtime'

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize xiaozhang.liquid_loading_downtime

# COMMAND ----------

df = spark.read.format('delta').load('/mnt/devadlstorage01/shared/xiaozhang/liquid_loading/downtime')
df.count()

# COMMAND ----------

downtime = spark.sql("select * from xiaozhang.liquid_loading_downtime")
print(downtime.count())
print(downtime.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Procount

# COMMAND ----------

#dbutils.fs.rm('/mnt/devadlstorage01/shared/xiaozhang/liquid_loading/PAwelldaily', True)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### PA daily

# COMMAND ----------

startDate = date(2018,1,1)
endDate = date(2022,4,1)
skip = 0 #days

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
    df.write.format('delta').mode('overwrite').save('/mnt/devadlstorage01/shared/xiaozhang/liquid_loading/PAwelldaily')     
  else:
    df.write.format('delta').mode('append').save('/mnt/devadlstorage01/shared/xiaozhang/liquid_loading/PAwelldaily')     
 
  i = i + 1
  
  curr = end
  end = end + timedelta(days=skip)
  
#display(df)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Producing Method & Status Description

# COMMAND ----------

#load in producing status and methods
q = "Select ProducingMethodsMerrickID, ProducingMethodsDescription as ProducingMethod from procount.ProducingMethodsTb"
producingmethod = QuerySqlDatabaseSelect('ODS',q)

q = "Select [ProducingStatusMerrickID], [ProducingDescription] as ProducingStatus from procount.ProducingStatusTb"
producingstatus = QuerySqlDatabaseSelect('ODS',q)

#load components just saved to datalake

pa_daily = spark.read.format('delta').load('/mnt/devadlstorage01/shared/xiaozhang/liquid_loading/PAwelldaily')

#join pa_daily, producingmethod and producingstatus
pa_daily_new = pa_daily.withColumnRenamed('ProducingMethod','ProducingMethodsMerrickID')\
.withColumnRenamed('ProducingStatus','ProducingStatusMerrickID')\
.join(producingmethod,'ProducingMethodsMerrickID','left').drop('ProducingMethodsMerrickID')\
.join(producingstatus,'ProducingStatusMerrickID','left').drop('ProducingStatusMerrickID')
         

pa_daily_new.write.format('delta').mode('overwrite').save('/mnt/devadlstorage01/shared/xiaozhang/liquid_loading/PAwelldailyJoined')

# COMMAND ----------

# MAGIC %sql
# MAGIC create table xiaozhang.liquid_loading_PAWellDailyJoined using delta location '/mnt/devadlstorage01/shared/xiaozhang/liquid_loading/PAwelldailyJoined'

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize xiaozhang.liquid_loading_PAWellDailyJoined

# COMMAND ----------

# MAGIC %md
# MAGIC ## Well 

# COMMAND ----------

#get well properties from Azure DWH
query = """
SELECT [ReportingStreamMerrick_id] as MerrickID
      ,[API10_id] as API10ID
      ,[Well_nm]
      ,[CompletionType_nm]
      ,[Spud_dt]
      ,[GasFirstProduction_dt]
      ,[OilFirstProduction_dt]
      ,[SurfaceLatitude_crd]
      ,[SurfaceLongitude_crd]
      ,[StateProvince_nm]
      ,[AccountingTeam_nm]
      ,[PA_Division_nm]
  FROM [mrt].[v_dim_WellReportingStream] as w
"""
'''well_dwh = (QuerySqlDatabaseSelect('DWH',query)
            .withColumn('Spud_dt',to_date(col("Spud_dt"),"yyyy-MM-dd"))
            .withColumn('GasFirstProduction_dt',to_date(col("GasFirstProduction_dt"),"yyyy-MM-dd"))
            .withColumn('OilFirstProduction_dt',to_date(col("OilFirstProduction_dt"),"yyyy-MM-dd"))
            .join(wv,'API10ID','left')
           )'''

well_dwh = (QuerySqlDatabaseSelect('DWH',query)
            .withColumn('Spud_dt',to_date(col("Spud_dt"),"yyyy-MM-dd"))
            .withColumn('GasFirstProduction_dt',to_date(col("GasFirstProduction_dt"),"yyyy-MM-dd"))
            .withColumn('OilFirstProduction_dt',to_date(col("OilFirstProduction_dt"),"yyyy-MM-dd"))            
           )



#display(well_dwh)

well_dwh.write.format('delta').mode('overwrite').save('/mnt/devadlstorage01/shared/xiaozhang/liquid_loading/well')

# COMMAND ----------

# MAGIC %sql
# MAGIC create table xiaozhang.liquid_loading_PAWell using delta location '/mnt/devadlstorage01/shared/xiaozhang/liquid_loading/well'

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize xiaozhang.liquid_loading_PAWell

# COMMAND ----------

# MAGIC %md
# MAGIC ## Wellview

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tubing Component

# COMMAND ----------

tubingcomponent_query = """
 SELECT left([WELLIDA],10) as API10ID
	  ,[IDREC] as TubingComponentID
      ,[IDRECPARENT] as TubingID
	  ,[DES] as Description
	  ,[SZODNOM] as OD
	  ,[WTPERLENGTH] as WTLength
	  ,[GRADE] as Grade
	  ,[JOINTS] as Joints
	  ,[LENGTH] as Length
	  ,[DEPTHTOPCALC] as DepthTop
	  ,[DEPTHBTMCALC] as DepthBottom
  FROM [wellview].[WVTUBCOMP] as a
  inner join [wellview].[WVWELLHEADER] as b
  on a.IDWell = b.IDWell
"""
tubingcomponent = (QuerySqlDatabaseSelect('ODS',tubingcomponent_query)
          .withColumn('WTLength',round(col('WTLength').cast('float'),3))
          .withColumn('OD',round(col('OD').cast('float'),3))
          #.withColumn('Joints',col('Joints').cast('Integer'))
          .withColumn('DepthTop',round(col('DepthTop').cast('float'),3))
          .withColumn('DepthBottom',round(col('DepthBottom').cast('float'),3))         
         )
#display(tubingcomponent)
tubingcomponent.write.format('delta').mode('overwrite').save('/mnt/devadlstorage01/shared/xiaozhang/liquid_loading/welltubingcomponent')

# COMMAND ----------

# MAGIC %sql
# MAGIC create table xiaozhang.liquid_loading_WVWellTubingComponent using delta location '/mnt/devadlstorage01/shared/xiaozhang/liquid_loading/welltubingcomponent'

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize xiaozhang.liquid_loading_WVWellTubingComponent

# COMMAND ----------

# MAGIC %md
# MAGIC ## Directional Survey

# COMMAND ----------

tmpA = spark.sql("select * from xiaozhang.liquid_loading_downtime") # has 3 years of data
tmpB = spark.sql("select * from xiaozhang.liquid_loading_PAWell")

# COMMAND ----------

tmp = tmpA.join(tmpB, how='inner', on=['MerrickID'])

# COMMAND ----------

tmp.select('PA_Division_nm').distinct().show()

# COMMAND ----------

API10 = tmp.filter(((tmp.AccountingTeam_nm == 'PERMIAN') | (tmp.AccountingTeam_nm == 'BAKKEN')) & (~tmp.PA_Division_nm.contains('CENTRAL'))).select('API10ID').toPandas()

# COMMAND ----------

API10 = pd.to_numeric(API10['API10ID'], errors='coerce', downcast='integer')

# COMMAND ----------

API10 = API10.dropna().astype('int').unique()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read directional survey

# COMMAND ----------

# read step data at a time
step = 50
count = 0
for i in range(0,API10.shape[0]-step,step):  
  
  APIloc = API10[range(i,i+step,1)]
  
  query = "SELECT [UWI_10], [MEASURED_DEPTH], [TV_DEPTH], [DEVIATION_ANGLE] FROM [da].[PIDM_DirSrvy] WHERE UWI_10 IN {}".format(tuple(APIloc))
  
  df = QuerySqlDatabaseSelect('DA',query)  
  
  n = df.count()
  
  print(i, n) 
  
  if n > 0:  
    df = df.withColumnRenamed("UWI_10","API10ID")    
    if count == 0:
      dbutils.fs.rm('/mnt/devadlstorage01/shared/xiaozhang/liquid_loading/dirSurvey', True)
      df.write.format('delta').mode('overwrite').save('/mnt/devadlstorage01/shared/xiaozhang/liquid_loading/dirSurvey')  
    else:    
      df.write.format('delta').mode('append').save('/mnt/devadlstorage01/shared/xiaozhang/liquid_loading/dirSurvey')  
      
    count = count + 1

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create table

# COMMAND ----------

# MAGIC %sql
# MAGIC create table xiaozhang.liquid_loading_directional_survey using delta location '/mnt/devadlstorage01/shared/xiaozhang/liquid_loading/dirSurvey'

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize xiaozhang.liquid_loading_directional_survey

# COMMAND ----------

df = spark.sql('SELECT * FROM xiaozhang.liquid_loading_directional_survey')

# COMMAND ----------

df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Undulation data 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Data from the file

# COMMAND ----------

dbutils.fs.rm('/mnt/devadlstorage01/shared/xiaozhang/liquid_loading/undulations', True)

# COMMAND ----------

dfPD = pd.read_csv("/dbfs/FileStore/shared_uploads/xiao.zhang@exxonmobil.com/undulations_asof_050322.csv")

# COMMAND ----------

dfPD = dfPD.rename(columns = {'API14ID' : 'UWI'})

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a spark dataframe and write to data lake

# COMMAND ----------

dfSpark = spark.createDataFrame(dfPD)
dfSpark.write.format('delta').mode('overwrite').save('/mnt/devadlstorage01/shared/xiaozhang/liquid_loading/undulations')  

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a table

# COMMAND ----------

# MAGIC %sql
# MAGIC create table xiaozhang.liquid_loading_undulations using delta location '/mnt/devadlstorage01/shared/xiaozhang/liquid_loading/undulations'

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize xiaozhang.liquid_loading_undulations

# COMMAND ----------

# MAGIC %md
# MAGIC ## Master well data from IHS

# COMMAND ----------

#dbutils.fs.rm('/mnt/devadlstorage01/shared/xiaozhang/liquid_loading/master_well', True)

# COMMAND ----------

df = pd.read_csv("/dbfs/FileStore/shared_uploads/amit.x.kumar@exxonmobil.com/UWI_Master.csv")

# COMMAND ----------

cols = ['UWI','UWI_12','UWI_10','HoleDirection','WellName','WellType','WellStatus','State','County','FieldName','Basin','SubBasin','PlayName','PlayType',\
        'AOI_XTO','Play_XTO','SubPlay_XTO','Lat','Lon','SpudDate','MD','TVD','LateralLength','FirstProdDate']

# COMMAND ----------

# drop columns

for c in df.columns:
  
  if c in cols:    
    continue
  else:
    df = df.drop(c, axis = 1)



# COMMAND ----------

df['SpudDate'] = pd.to_datetime(df['SpudDate'])
df['FirstProdDate'] = pd.to_datetime(df['FirstProdDate'])

# COMMAND ----------

dfSpark = spark.createDataFrame(df)
dfSpark.write.format('delta').mode('overwrite').save('/mnt/devadlstorage01/shared/xiaozhang/liquid_loading/master_well')  

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create table

# COMMAND ----------

# MAGIC %sql
# MAGIC create table xiaozhang.liquid_loading_master_well using delta location '/mnt/devadlstorage01/shared/xiaozhang/liquid_loading/master_well'

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize xiaozhang.liquid_loading_master_well

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tubing Install Data 

# COMMAND ----------

tubinginstall_query = """
 SELECT
       c.[MerrickID]
         ,left(c.[ApiWellNumber],10) as "API10ID"
      ,c.[WellName]
         ,d.[DivisionName] as "Division"
         ,a.[AreaName] as "Area"
      ,frmp.[PersonnelName] as "Foreman"
         ,pm.[ProducingMethodsDescription] as "ProducingMethod"
         ,ps.[ProducingDescription] as "ProducingStatus"
         ,h.[IDWELL]
         ,wv.[DEPTHBTM]
         ,wv.[DTTMRUN]
         
  FROM [procount].[CompletionTb] as c

  left join [procount].[DivisionTb] as d
  on c.[DivisionID] = d.[DivisionMerrickID]

  left join [procount].[AreaTb] as a
  on c.[AreaID] = a.[AreaMerrickID]

  Left JOIN [procount].[ProducingMethodsTb] as pm
  on c.[ProducingMethod] = pm.[ProducingMethodsMerrickID]

  Left JOIN [procount].[ProducingStatusTb] as ps
  on c.[ProducingStatus] = ps.[ProducingStatusMerrickID]

  Left join [procount].[PersonnelTb] as frmp
  on c.[ForemanPersonID] = frmp.[MerrickID]

  left join (SELECT [IDWELL],left([WELLIDA],10) as'API10'FROM [wellview].[WVWELLHEADER]) as h
  on left(c.[ApiWellNumber],10) = h.[API10]

  left join [wellview].[wvtub] as wv
  on h.[IDWELL] = wv.[IDWELL]


  where d.DivisionName like '%BAK%' 
  --and wv.[DEPTHBTM] is null 
  --and pm.[ProducingMethodsDescription] like '%flow%'
"""

tubinginstall = QuerySqlDatabaseSelect('ODS',tubinginstall_query)
#                  .withColumn('DTTMRUN',to_date(col("DTTMRUN"),"yyyy-MM-dd"))

display(tubinginstall)

# COMMAND ----------

dbutils.fs.rm('/mnt/devadlstorage01/shared/xiaozhang/liquid_loading/welltubinginstall', True)

# COMMAND ----------

tubinginstall.write.format('delta').mode('overwrite').save('/mnt/devadlstorage01/shared/xiaozhang/liquid_loading/welltubinginstall')

# COMMAND ----------

# MAGIC %sql
# MAGIC create table xiaozhang.WellTubingInstall using delta location '/mnt/devadlstorage01/shared/xiaozhang/liquid_loading/welltubinginstall'

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize xiaozhang.WellTubingInstall
