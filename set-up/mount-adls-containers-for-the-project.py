# Databricks notebook source
# MAGIC %md
# MAGIC 1. define a funtion to mount the containers. the fucntion will perform following steps
# MAGIC 2. get client id, tenant id, client secret stored in Azure Key vault
# MAGIC 3. Set spark configuration using the service principal as a dictionary
# MAGIC 4. Mount the storage account and container to the DBFS
# MAGIC 5. read data from the file stored in adls

# COMMAND ----------

def mount_container(storageaccount,container):
    # get secrets stored in Azure Key Vault
    clientid = dbutils.secrets.get(scope='formula1-account-key',key='formula1-app-client-id')
    tenantid = dbutils.secrets.get(scope='formula1-account-key',key='formula1-app-tenant-id')
    client_secret= dbutils.secrets.get(scope='formula1-account-key',key='formula1-app-client-secret')
    
    # save the spark configuration as a dictionary
    config = {"fs.azure.account.auth.type": "OAuth",
        "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
	    "fs.azure.account.oauth2.client.id": clientid,
	    "fs.azure.account.oauth2.client.secret": client_secret,
	    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenantid}/oauth2/token"}

    #unmount the storage if exists
    if any(mount.mountPoint == f"/mnt/{storageaccount}/{container}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storageaccount}/{container}")


    # mounting the storage to the DBFS
    dbutils.fs.mount(
    source=f"abfss://{container}@{storageaccount}.dfs.core.windows.net/", 
    mount_point=f"/mnt/{storageaccount}/{container}",
    extra_configs=config)

    # display the mount points
    display(dbutils.fs.mounts())


# COMMAND ----------

mount_container('projformula1dl','raw')

# COMMAND ----------

mount_container('projformula1dl','processed')

# COMMAND ----------

mount_container('projformula1dl','presentation')