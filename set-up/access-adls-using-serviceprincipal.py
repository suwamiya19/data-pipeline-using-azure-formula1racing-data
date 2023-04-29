# Databricks notebook source
# MAGIC %md
# MAGIC 1. Using AAD, create service principal and get client id, tenant id, client secret
# MAGIC 2. get the secrets stored in azure key vault
# MAGIC 3. Set spark configuration using the service principal 
# MAGIC 4. Use ABFS (Azure Blob File System) driver to access the data stored in storage account
# MAGIC 5. read data from the file stored in adls

# COMMAND ----------

clientid = dbutils.secrets.get(scope='formula1-account-key',key='formula1-app-client-id')
tenantid = dbutils.secrets.get(scope='formula1-account-key',key='formula1-app-tenant-id')
client_secret= dbutils.secrets.get(scope='formula1-account-key',key='formula1-app-client-secret')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.projformula1dl.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.projformula1dl.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.projformula1dl.dfs.core.windows.net", clientid)
spark.conf.set("fs.azure.account.oauth2.client.secret.projformula1dl.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.projformula1dl.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenantid}/oauth2/token")

# COMMAND ----------

dbutils.fs.ls("abfss://demo@projformula1dl.dfs.core.windows.net")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@projformula1dl.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@projformula1dl.dfs.core.windows.net/circuits.csv"))