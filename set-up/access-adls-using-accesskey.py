# Databricks notebook source
# MAGIC %md
# MAGIC 1. Set spark configuration for fs.azure.account.key
# MAGIC 2. Use ABFS (Azure Blob File System) driver to access the data stored in storage account
# MAGIC 3. read data from the file stored in adls

# COMMAND ----------

dbutils.secrets.list(scope = 'formula1-account-key')

# COMMAND ----------

accountKey = dbutils.secrets.get(scope='formula1-account-key',key='formula1-account-key')

# COMMAND ----------

spark.conf.set("fs.azure.account.key.projformula1dl.dfs.core.windows.net",accountKey)

# COMMAND ----------

dbutils.fs.ls("abfss://demo@projformula1dl.dfs.core.windows.net")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@projformula1dl.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@projformula1dl.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

