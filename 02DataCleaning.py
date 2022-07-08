# Databricks notebook source
# MAGIC %run "Config/Functions"

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE SCHEMA IF NOT EXISTS ESP_sandbox;

# COMMAND ----------

write_database_name = 'ESP_sandbox'
write_table_name = 'ODS'
mnt_location = '/mnt/devadlstorage01/shared/ESP_Xiao/'
create_table_command = f'''
      CREATE TABLE {write_database_name}.{write_table_name}
      USING DELTA LOCATION '{mnt_location}'
      '''
spark.sql(create_table_command)

# COMMAND ----------


