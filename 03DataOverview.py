# Databricks notebook source
# Databricks notebook source
import pandas as pd
from pyspark.sql.functions import desc, col, count, countDistinct, min, max, sum
import matplotlib.pyplot as plt
import numpy as np
from plotly.subplots import make_subplots
import plotly.graph_objects as go
import os

# COMMAND ----------

downtime_query = """
Select 
distinct(DowntimeCode),
sum(DowntimeHours) as totalDowntimeHours,
sum(OilLostProduction) as totalOilLost,
sum(GasLostProduction) as totalGasLost
from agame.pa_welldowntime 
group by DowntimeCode
"""
downtime_agg = spark.sql(downtime_query)

# COMMAND ----------

downhours = downtime_agg.toPandas().sort_values(by='totalDowntimeHours', ascending = True)
plt.figure(figsize=(16,10))
plt.barh(downhours.tail(10).DowntimeCode.astype(str), downhours.tail(10).totalDowntimeHours)
plt.xlabel('Downtime Hours'), plt.ylabel('Downtime Code')
plt.title('Unconventional Downtime Hours Ranked by Severity')
plt.show()

# COMMAND ----------

display(downhours)

# COMMAND ----------

# well = spark.table("agame.pa_well").select('MerrickID','PA_Division_nm','API10ID', 'WellConfig','SurfaceLatitude_crd', 'SurfaceLongitude_crd', 'meternrt').filter(col('PA_Division_nm')=='FTW BARNETT SHALE').filter(col('WellConfig')=='Horizontal').filter(col('meternrt')>0)
well = spark.table("agame.pa_well").select('MerrickID','PA_Division_nm','API10ID', 'WellConfig','SurfaceLatitude_crd', 'SurfaceLongitude_crd', 'meternrt')
display(well)

# COMMAND ----------

welldown = spark.table("agame.pa_welldowntime")
well_downtime = (well.select('MerrickID', 'API10ID','SurfaceLatitude_crd', 'SurfaceLongitude_crd', 'PA_Division_nm')
               .join(welldown, on='MerrickID', how='inner')
              )
well_down = well_downtime.toPandas()
efwells = list(well_down[well_down.DowntimeCode==101].API10ID.unique())

# COMMAND ----------

display(well_downtime)

# COMMAND ----------

write_database_name = 'ESP_sandbox'
write_table_name = 'well_downtime_agg'
mnt_location = '/mnt/devadlstorage01/shared/ESP_Xiao/'
write_format = 'delta'
write_mode = 'overwrite'
write_table_name = 'well_downtime'


def writeToDBFS(df):
    print('start writing to dbfs')
    df.write.format(write_format).\
      mode(write_mode).\
      option("overwriteSchema", "true").\
      save(mnt_location)
    print('finish writing to dbfs')
        
def createDeltaTable():
    try:
        drop_table_command =f'DROP TABLE {write_table_location}.{write_table_name}'
        spark.sql(drop_table_command)
    except:
        print('Table not exist')
    print('Start creating the table')
    create_table_command = f'''
                            CREATE TABLE {write_database_name}.{write_table_name}
                            USING DELTA LOCATION '{mnt_location}'
                            '''
    spark.sql(create_table_command)
    print('Finish creating the table')


writeToDBFS(well_downtime)
createDeltaTable()

# COMMAND ----------

well_down.head()

# COMMAND ----------

well_down.describe()

# COMMAND ----------

well_down =well_down[well_down['DowntimeCode']==101]

# COMMAND ----------

# make aggregate data over the wells
apis, down_incidents, down_codes, down_hours, lat, long = [], [], [], [], [], []

for api in well_down.API10ID.unique():
  temp = well_down[well_down.API10ID==api]
  apis.append(api)
  down_incidents.append(len(temp))
  down_codes.append(len(temp.DowntimeCode.unique()))
  down_hours.append(temp.DowntimeHours.sum())
  lat.append(temp.SurfaceLatitude_crd.values[0])
  long.append(temp.SurfaceLongitude_crd.values[0])

welldownagg = {'API10ID':apis, 'down_incidents':down_incidents, 'down_codes':down_codes, 'down_hours':down_hours, 'surf_lat':lat, 'surf_long':long}
welldownaggdf = pd.DataFrame.from_dict(welldownagg, orient='columns')

# fig= plt.figure(figsize=(20,8))
# plt.scatter(welldownaggdf.down_incidents, welldownaggdf.down_codes, c=welldownaggdf.down_hours, alpha=0.5), plt.xlabel('number of downtime events'), plt.ylabel('number of unique codes'), plt.colorbar()
# plt.show()

# COMMAND ----------

welldownaggdf.head()

# COMMAND ----------

fig= plt.figure(figsize=(20,12))
plt.scatter(welldownaggdf.surf_long, welldownaggdf.surf_lat, c=welldownaggdf.down_hours.rank(pct=True), s=welldownaggdf.down_incidents, cmap='cividis', alpha=0.5)
plt.xlabel('longitude')
plt.ylabel('latitude')
plt.colorbar(label='downtime hours percentile')
print('Percentiles', [(val, np.percentile(welldownaggdf.down_hours, np.array([val]))) for val in [1, 25, 50, 75, 99]])

# COMMAND ----------


