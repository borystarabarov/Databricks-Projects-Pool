# Databricks notebook source
df = spark.read.format("parquet") \
               .load("abfss://external-location@databricksdevstgacc.dfs.core.windows.net/files/")

# COMMAND ----------

display(df) 

# COMMAND ----------

display(df) 
