# Databricks notebook source
v_result = dbutils.notebook.run("ingest-circuits-csv", 0, {"data_source": "Ergast API","file_date":"2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("ingest-races-csv", 0, {"data_source": "Ergast API","file_date":"2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("ingest-constructors-json", 0, {"data_source": "Ergast API","file_date":"2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("ingest-drivers-json", 0, {"data_source": "Ergast API","file_date":"2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("ingest-results-json", 0, {"data_source": "Ergast API","file_date":"2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("ingest-pitstops-json", 0, {"data_source": "Ergast API","file_date":"2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("ingest-laptimes-json", 0, {"data_source": "Ergast API","file_date":"2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("ingest-qualifying-json", 0, {"data_source": "Ergast API","file_date":"2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC race_id,count(1)
# MAGIC FROM processed_f1.qualifying
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC