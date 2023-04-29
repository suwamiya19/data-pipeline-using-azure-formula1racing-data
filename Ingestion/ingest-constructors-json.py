# Databricks notebook source
# MAGIC %md
# MAGIC 1. Read json file from raw
# MAGIC 2. rename and add required columns
# MAGIC 3. write the final dataframe as parquet to processed folder

# COMMAND ----------

dbutils.widgets.text('data_source','')
v_data_source = dbutils.widgets.get('data_source')

# COMMAND ----------

dbutils.widgets.text('file_date','2021-03-21')
v_file_date=dbutils.widgets.get('file_date')

# COMMAND ----------

# MAGIC %run "../Includes/configurations"

# COMMAND ----------

# MAGIC %run "../Includes/common-functions"

# COMMAND ----------

# MAGIC %md
# MAGIC **Defining Schema**

# COMMAND ----------

schema = 'constructorId INT, constructorRef STRING,name STRING,nationality STRING, url STRING'

# COMMAND ----------

# MAGIC %md
# MAGIC **Read data from JSON**

# COMMAND ----------

constructor_df = spark.read.schema(schema).json(f'{raw_folder}/{v_file_date}/constructors.json')

# COMMAND ----------

# MAGIC %md
# MAGIC **Rename the column**

# COMMAND ----------

from pyspark.sql.functions import lit
constructor_renamed_df=constructor_df.withColumnRenamed('constructorId','constructor_id')\
                        .withColumnRenamed('constructorRef','constructor_ref')\
                            .withColumn('data_source',lit(v_data_source))\
                            .withColumn('file_date',lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC **Dropping the unnecessary column**

# COMMAND ----------

constructor_dropped_df=constructor_renamed_df.drop(constructor_renamed_df['url'])

# COMMAND ----------

# MAGIC %md
# MAGIC **Add ingestion date**

# COMMAND ----------


constructor_final_df=addIngestionDate(constructor_dropped_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Write data as parquet**

# COMMAND ----------

constructor_final_df.write.mode('overwrite').format("delta").saveAsTable("processed_f1.constructors")

# COMMAND ----------

dbutils.notebook.exit('Success')