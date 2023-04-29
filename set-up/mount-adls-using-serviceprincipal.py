# Databricks notebook source
# MAGIC %md
# MAGIC 1. get client id, tenant id, client secret stored in Azure Key vault
# MAGIC 2. Set spark configuration using the service principal as a dictionary
# MAGIC 3. Mount the storage account and container to the DBFS
# MAGIC 4. read data from the file stored in adls

# COMMAND ----------

# get secrets stored in Azure Key Vault

clientid = dbutils.secrets.get(scope='formula1-account-key',key='formula1-app-client-id')
tenantid = dbutils.secrets.get(scope='formula1-account-key',key='formula1-app-tenant-id')
client_secret= dbutils.secrets.get(scope='formula1-account-key',key='formula1-app-client-secret')

# COMMAND ----------

# save the spark configuration as a dictionary

config = {"fs.azure.account.auth.type": "OAuth",
        "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
	    "fs.azure.account.oauth2.client.id": clientid,
	    "fs.azure.account.oauth2.client.secret": client_secret,
	    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenantid}/oauth2/token"}
        

# COMMAND ----------

# mounting the storage to the DBFS

dbutils.fs.mount(
    source="abfss://demo@projformula1dl.dfs.core.windows.net/", 
    mount_point="/mnt/projformula1dl/demo",
    extra_configs=config)


# COMMAND ----------

# display the mount points

display(dbutils.fs.mounts())

# COMMAND ----------

display(dbutils.fs.ls('/mnt/projformula1dl/demo'))

# COMMAND ----------

display(spark.read.csv("/mnt/projformula1dl/demo/circuits.csv"))