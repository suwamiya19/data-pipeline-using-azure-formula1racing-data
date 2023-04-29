# Databricks notebook source
# MAGIC %md
# MAGIC 1. Ingestion Data fucntion

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

def addIngestionDate(input_df):
    output_df = input_df.withColumn('Ingestion_Date',current_timestamp())
    return output_df

# COMMAND ----------

# MAGIC %md
# MAGIC 2. re-arrange partition column function

# COMMAND ----------

def re_arrange_partition_column(input_df, partition_column):
  column_list = []
  for column_name in input_df.schema.names:
    if column_name != partition_column:
      column_list.append(column_name)
  column_list.append(partition_column)
  output_df = input_df.select(column_list)
  return output_df

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Overwrite partition function

# COMMAND ----------

def overwritePartition(input_df, db_name, table_name, partition_column):
  output_df = re_arrange_partition_column(input_df, partition_column)
  spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
  if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
    output_df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
  else:
    output_df.write.mode("overwrite").partitionBy(partition_column).format("parquet").saveAsTable(f"{db_name}.{table_name}")


# COMMAND ----------

# MAGIC %md
# MAGIC 4. Convert column to list

# COMMAND ----------

def columnToList(input_df, column_name):
  df_row_list = input_df.select(column_name).distinct().collect()
  column_value_list = [row[column_name] for row in df_row_list]
  return column_value_list