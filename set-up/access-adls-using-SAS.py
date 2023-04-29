# Databricks notebook source
# MAGIC %md
# MAGIC 1. Set spark configuration for SAS token
# MAGIC 2. Use ABFS (Azure Blob File System) driver to access the data stored in storage account
# MAGIC 3. read data from the file stored in adls

# COMMAND ----------

sasToken = dbutils.secrets.get(scope='formula1-account-key',key='formula1-demo-sas-token')

# COMMAND ----------

# define auth type as SAS
spark.conf.set("fs.azure.account.auth.type.projformula1dl.dfs.core.windows.net", "SAS")

# define SAS token provider as Fixed
spark.conf.set("fs.azure.sas.token.provider.type.projformula1dl.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")

# set value of SAS token 
spark.conf.set("fs.azure.sas.fixed.token.projformula1dl.dfs.core.windows.net", sasToken )

# COMMAND ----------

dbutils.fs.ls("abfss://demo@projformula1dl.dfs.core.windows.net")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@projformula1dl.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@projformula1dl.dfs.core.windows.net/circuits.csv"))