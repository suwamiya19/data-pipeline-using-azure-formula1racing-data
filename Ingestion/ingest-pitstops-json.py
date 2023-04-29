# Databricks notebook source
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
# MAGIC
# MAGIC **Define Schema**

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("stop", StringType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("duration", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])


# COMMAND ----------

# MAGIC %md
# MAGIC **Reading multi line JSON**

# COMMAND ----------

pitstops_df=spark.read.schema(schema).option('multiLine',True).json(f'{raw_folder}/{v_file_date}/pit_stops.json')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC *rename and add column**

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

final_df = pitstops_df.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
    .withColumn('data_source',lit(v_data_source)) \
        .withColumn("file_date", lit(v_file_date))

final_df=addIngestionDate(final_df) 

# COMMAND ----------

# MAGIC %md
# MAGIC **Write the parquet file to processed folder**

# COMMAND ----------

overwritePartition(final_df,"processed_f1","pit_stops","race_id")

# COMMAND ----------

display(spark.read.parquet(f'{processed_folder}/pit_stops'))

# COMMAND ----------

dbutils.notebook.exit('Success')