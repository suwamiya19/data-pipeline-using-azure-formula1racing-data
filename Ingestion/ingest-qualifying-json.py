# Databricks notebook source
dbutils.widgets.text('data_source','')
v_data_source = dbutils.widgets.get('data_source')

# COMMAND ----------

dbutils.widgets.text('file_date','2021-03-21')
v_file_date = dbutils.widgets.get('file_date')

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


schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                      StructField("raceId", IntegerType(), True),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("constructorId", IntegerType(), True),
                                      StructField("number", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("q1", StringType(), True),
                                      StructField("q2", StringType(), True),
                                      StructField("q3", StringType(), True),
                                     ])


# COMMAND ----------

# MAGIC %md
# MAGIC **Reading multi line JSON**

# COMMAND ----------

qualifying_df = spark.read \
.schema(schema) \
.option("multiLine", True) \
.json(f"{raw_folder}/{v_file_date}/qualifying")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **rename and add column**

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

final_df = qualifying_df.withColumnRenamed("qualifyId", "qualify_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("constructorId", "constructor_id") \
    .withColumn('data_source',lit(v_data_source))\
        .withColumn('file_date',lit(v_file_date))

final_df=addIngestionDate(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Write the parquet file to processed folder**

# COMMAND ----------

overwritePartition(final_df,"processed_f1","qualifying","race_id")

# COMMAND ----------

display(spark.read.parquet(f'{processed_folder}/qualifying'))

# COMMAND ----------

dbutils.notebook.exit('Success')