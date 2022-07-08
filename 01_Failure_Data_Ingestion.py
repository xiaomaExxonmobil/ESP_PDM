# Databricks notebook source
# DBTITLE 1,Configure
# MAGIC %run "/Config/Functions"

# COMMAND ----------

def createDeltaTable(mnt_location, df=None, db_name=None, table_name=None, write_mode='overwrite', write_format = 'delta'):
  if df is not None:
    print('start writing to dbfs')
    df.write.format(write_format).mode(write_mode).save(mnt_location)
    print('finish writing to dbfs')
    try:
        drop_table_command =f'DROP TABLE {db_name}.{table_name}'
        spark.sql(drop_table_command)
    except:
        print('Table not exist')
    print('Start creating the table')
  
  create_db_command = f'''
                      CREATE DATABASE IF NOT EXISTS {db_name}
                      '''
  create_table_command = f'''
                          CREATE TABLE {db_name}.{table_name} USING DELTA LOCATION '{mnt_location}'
                          '''
  spark.sql(create_db_command)
  spark.sql(create_table_command)
  print('Finish creating the table')  

# COMMAND ----------

FOLDER_PATH = r'/mnt/devadlstorage01/shared/xiao_ma/esp/'
MASTER_TABLE_FILE_PATH = FOLDER_PATH + 'esp_master'
FAILURE_TABLE_FILE_PATH = FOLDER_PATH + 'esp_failure'
DOWNTIME_TABLE_FILE_PATH = FOLDER_PATH + 'esp_downtime'

# COMMAND ----------

# MAGIC %md ##Master Table

# COMMAND ----------

sql = """

SELECT
       c.[MerrickID]
	  ,left(c.[ApiWellNumber],10) as "API10"
      ,c.[WellName]
      ,c.Latitude
      ,c.Longitude
	  ,d.[DivisionName] as "Division"
	  ,a.[AreaName] as "Area"
	  ,pm.[ProducingMethodsDescription] as "ProducingMethod"
	  ,ps.[ProducingDescription] as "Status"
	  ,h.[IDWELL]
	  
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

  Left join [procount].[PersonnelTb] as eng
  on c.[EngineerPersonID] = eng.[MerrickID]

  left join (SELECT [IDWELL],left([WELLIDA],10) as'API10'FROM [wellview].[WVWELLHEADER]) as h
  on left(c.[ApiWellNumber],10) = h.[API10]


  where pm.[ProducingMethodsDescription] like '%ESP%'
  
  """
  
df_esp_master = QuerySqlDatabaseSelect('ODS',sql)
display(df_esp_master)

# COMMAND ----------

createDeltaTable(df_esp_master,db_name='esp_xiao', table_name='esp_master', mnt_location = MASTER_TABLE_FILE_PATH)

# COMMAND ----------

# MAGIC %md ##Wellview Failure

# COMMAND ----------

sql ="""

SELECT DISTINCT 

----Problem table WVPROBLEM
	Problem.idwell as "Problem_ID_Well", LEFT(WellIda,10) as 'API10', Problem.idrec, Problem.Typ "Problem_Type",
	CASE  When upper(Problem.typ)  LIKE 'NON-FAILURE%' then 'Include Non-Failures' else 'Exclude Non-Failures'  END as [Failure-Filter],
	Problem.UserTxt3 AS "Lift_Method",
	Problem.cause,
	Problem.CAUSEDETAIL,
	Problem.com,
	actiontaken,
	CASE WHEN row_number () over (partition by problem.idwell order by problem.dttmstart asc)=1 then dttmruncalc
	ELSE lag(Problem.DTTMEND,1) over (partition by problem.idwell order by problem.dttmstart asc) end as "Problem_Start",
	Problem.dttmstart AS "Failure_End_Date",
	DATEDIFF(DAY, case when row_number () over (partition by problem.idwell order by problem.dttmstart asc)=1 then dttmruncalc 
	else lag(Problem.DTTMEND,1) over (partition by problem.idwell order by problem.dttmstart asc) end
	,PROBLEM.DtTMSTART) AS "Run_Days",
	p1.depth as FailureDepth,
	problem.estcost as "AFE_Cost",
	Problem.DTTMACTION AS "Action_Date",
	Problem.DTTMEND AS "Resolved_Date",
	DurEndToActionCalc AS"Action_to_Resolution_Duration",
	DurActionToStartCalc AS "Failure_to_Action_Duration",
	DurEndToStartCalc "Failure_to_Resolution_Duration",
	DurJobStartToStartCalc "Failure_to_Job_Start_Duration",
	----------------WVPROBLEM DETAILS        
	dttmpullcalc,
	dttmruncalc,
	p1.durstarttoruncalc,
	pullreasoncalc ,
	------------------------Job wvjob
	job.costtotalcalc "Field_Estimate_Cost",
	job.dttmstart as jobstart,
	job.dttmend as Jobend,

	------------------- Wellheader wvwelleader
	Head.WellIDB AS sapid,
	Head.WellIDE AS accountingid,
	wellname,
	Head.idwell,
	Head.welltyp2,
	Head.fieldname,
	basin,
	Division,
	area,
	country,
	county,
	currentwellstatus1

FROM wellview.WVPROBLEM as Problem
LEFT JOIN wellview.wvproblemdetail as p1 ON Problem.idrec=p1.idrecparent
LEFT JOIN wellview.wvjob as job ON job.idrec= Problem.idrecjob
LEFT JOIN wellview.wvwellheader as Head ON Head.idwell=problem.idwell
where year(Problem.dttmstart)>2010
"""

df_failure = QuerySqlDatabaseSelect('ODS',sql)
display(df_failure)

# COMMAND ----------

createDeltaTable(df_failure,db_name='esp_xiao', table_name='esp_failure', mnt_location = FAILURE_TABLE_FILE_PATH)

# COMMAND ----------

# MAGIC %md ##Downtime

# COMMAND ----------

# break the query into smaller queries to prevent timeout
startDate = date(2019,1,1)
endDate = date(2021,12,31)

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
                  .withColumn('DowntimeCode',col('DowntimeCode').cast('Integer')))
  for column in ['DowntimeHours','OilLostProduction','GasLostProduction','WaterLostProduction']:
    ods_downtime = ods_downtime.withColumn(column,col(column).cast('double'))
  for column in ['DowntimeReasonDescription','DowntimeComments','LostProdCalculationType']:
    ods_downtime = ods_downtime.withColumn(column,col(column).cast('string'))  
  #file_path = r'/mnt/devadlstorage01/shared/xiao_ma/esp_failure/downtime/'
  file_path = DOWNTIME_TABLE_FILE_PATH
  ods_downtime.write.format('delta').mode('append').save(file_path) 

# COMMAND ----------

createDeltaTable(mnt_location = DOWNTIME_TABLE_FILE_PATH, db_name='esp_xiao', table_name='esp_downtime')
