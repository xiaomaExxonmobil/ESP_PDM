# Databricks notebook source
# DBTITLE 1,Configure
# MAGIC %run "/Config/Functions"

# COMMAND ----------

sql = """




SELECT
       c.[WellName]
	  ,d.[DivisionName] as "Division"
	  ,a.[AreaName] as "Area"
	  ,s.[StateName] as "State"
	  ,ct.[CountyName] as "County"
	  ,f.[FieldGroupName] as "Field"
	  ,frm.[FormationName] as "Formation"
	  ,dda.[DDAFieldName] as "DDA"
	  ,b.[BatteryDescription] as "Battery"
	  ,sup.[PersonnelName] as "Superintendent"
	  ,frmp.[PersonnelName] as "Foreman"
	  ,pp.[PersonnelName] as "Lease Operator"
	  ,e.[PersonnelName] as "Engingeer"
	  ,ro.[RouteName] as "Route"
	  ,pm.[ProducingMethodsDescription] as "ProducingMethod"
	  ,ps.[ProducingDescription] as "ProductionStatus"
      ,c.[OutsideOperatedFlag] as "OutsideOperated"
	  ,c.[Latitude]
      ,c.[Longitude]
	  ,c.[MerrickID]
	  ,c.[ApiWellNumber]
  FROM [procount].[CompletionTb] as c

  left join [procount].[DivisionTb] as d
  on c.[DivisionID] = d.[DivisionMerrickID]

  left join [procount].[AreaTb] as a
  on c.[AreaID] = a.[AreaMerrickID]

  left join [procount].[StateNamesTb] as s
  on c.[StateID] = s.[StateCode]

  left join [procount].[StateCountyNamesTb] as ct
  on c.[CountyID] = ct.[CountyCode]

  left join [procount].[FieldGroupTb] as f
  on c.[FieldGroupID] = f.[FieldGroupMerrickID]

  left join [procount].[FormationTb] as frm
  on c.[FormationID] = frm.[FormationMerrickID]

  left join [procount].[DDATb] as dda
  on c.[DDAID] = dda.[DDAFieldMerrickID]

  left join [procount].[BatteryTb] as b
  on c.[BatteryID] = b.[BatteryMerrickID]

  Left join [procount].[PersonnelTb] as sup
  on c.[SuperPersonID] = sup.[MerrickID]

  Left join [procount].[PersonnelTb] as frmp
  on c.[ForemanPersonID] = frmp.[MerrickID]

  Left join [procount].[PersonnelTb] as pp
  on c.[PumperPersonID] = pp.[MerrickID]

  Left join [procount].[PersonnelTb] as e
  on c.[EngineerPersonID] = e.[MerrickID]

  left join [procount].[RouteTb] as ro
  on c.[RouteID] = ro.[RouteMerrickID]

  Left JOIN [procount].[ProducingMethodsTb] as pm
  on c.[ProducingMethod] = pm.[ProducingMethodsMerrickID]

  left join [procount].[ProducingStatusTb] as ps
  on c.[ProducingStatus] = ps.[ProducingStatusMerrickID]




"""

df = QuerySqlDatabaseSelect('ODS',sql)
display(df)

# COMMAND ----------

sql = """

SELECT
       c.[MerrickID]
	  ,left(c.[ApiWellNumber],10) as "API10"
      ,c.[WellName]
	  ,d.[DivisionName] as "Division"
	  ,a.[AreaName] as "Area"
      ,frmp.[PersonnelName] as "Foreman"
	  ,eng.[PersonnelName] as "Engineer"
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

sql ="""

SELECT DISTINCT 

----Problem table WVPROBLEM
				Problem.idwell as "Problem ID Well",
LEFT(WellIda,10) WellIda,
                Problem.idrec,
                Problem.Typ "Problem Type",
				CASE  
When upper(Problem.typ)  LIKE 'NON-FAILURE%' then 'Include Non-Failures' else 'Exclude Non-Failures'  END as [Failure-Filter],
                Problem.UserTxt3 AS "Lift Method",
                Problem.cause,
                Problem.CAUSEDETAIL,
                Problem.com,
                actiontaken,
				case when
 row_number () over (partition by problem.idwell order by problem.dttmstart asc)=1 then dttmruncalc
 else lag(Problem.DTTMEND,1) over (partition by problem.idwell order by problem.dttmstart asc) end as "Problem Start",

 
                Problem.dttmstart AS "Failure End Date",
		DATEDIFF(DAY,
						case when
 row_number () over (partition by problem.idwell order by problem.dttmstart asc)=1 then dttmruncalc
 else lag(Problem.DTTMEND,1) over (partition by problem.idwell order by problem.dttmstart asc) end
 ,PROBLEM.DtTMSTART) AS "Run Days"
 ,p1.depth as FailureDepth
 ,
                problem.estcost as "AFE Cost",
                Problem.DTTMACTION AS "Action Date",
                Problem.DTTMEND AS "Resolved Date",
                DurEndToActionCalc AS"Action to Resolution Duration",
                DurActionToStartCalc AS "Failure to Action Duration",
                DurEndToStartCalc "Failure to Resolution Duration",
                DurJobStartToStartCalc "Failure to Job Start Duration",

				   ----------------WVPROBLEM DETAILS        
		        dttmpullcalc,
                dttmruncalc,
                p1.durstarttoruncalc,
                pullreasoncalc ,
				p1.InclTopCalc TopInclination,
				p1.InclBTMCalc BottomInclination,
				p1.InclMaxCalc MaxInclination,

------------------------Job wvjob
				job.costtotalcalc "Field Estimate Cost",
job.dttmstart as jobstart
,job.dttmend as Jobend,

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

display(df_failure)

# COMMAND ----------

display(df_esp_master)

# COMMAND ----------

dbutils.fs.rm('dbfs:/mnt/devadlstorage01/shared/ESP_Xiao', recurse=True)
#dbutils.fs.rm(AD_1S_STAGE, recurse=True)


# COMMAND ----------

write_format = 'delta'
write_mode = 'overwrite'
write_table_name = 'esp_master'
mnt_location = f'/mnt/devadlstorage01/shared/ESP_Xiao/{write_table_name}'
write_table_location = 'ESP_sandbox'

def createDeltaTable(df):
  print('start writing to dbfs')
  df.write.format(write_format).mode(write_mode).save(mnt_location)
  print('finish writing to dbfs')
  try:
      drop_table_command =f'DROP TABLE {write_table_location}.{write_table_name}'
      spark.sql(drop_table_command)
  except:
      print('Table not exist')
  print('Start creating the table')
  create_table_command = f'''
                          CREATE TABLE {write_table_location}.{write_table_name}
                          USING DELTA LOCATION '{mnt_location}'
                          '''
  spark.sql(create_table_command)
  print('Finish creating the table')
  
createDeltaTable(df_esp_master)

# COMMAND ----------

import re
def remove_invalid_characters(df):
  orig_columns=df.columns
  new_columns = [re.sub("[^a-zA-Z0-9_\n\.]","",x) for x in orig_columns]
  new_columns1=[]
  for column in new_columns:
    if column not in new_columns1:
      new_columns1.append(column)
    else:
      new_columns1.append(i+'_dup')
  final_columns=['`'+i +'`'+' as '+j for i,j in zip(orig_columns,new_columns1)]
  df=df.selectExpr(*final_columns)
  return df
df_failure = remove_invalid_characters(df_failure)

# COMMAND ----------

display(df_failure)

# COMMAND ----------

write_format = 'delta'
write_mode = 'overwrite'
write_table_name = 'esp_failure'
mnt_location = f'/mnt/devadlstorage01/shared/ESP_Xiao/{write_table_name}'
write_table_location = 'ESP_sandbox'

def createDeltaTable(df):
  print('start writing to dbfs')
  df.write.format(write_format).mode(write_mode).save(mnt_location)
  print('finish writing to dbfs')
  try:
      drop_table_command =f'DROP TABLE {write_table_location}.{write_table_name}'
      spark.sql(drop_table_command)
  except:
      print('Table not exist')
  print('Start creating the table')
  create_table_command = f'''
                          CREATE TABLE {write_table_location}.{write_table_name}
                          USING DELTA LOCATION '{mnt_location}'
                          '''
  spark.sql(create_table_command)
  print('Finish creating the table')
  
createDeltaTable(df_failure)

# COMMAND ----------


