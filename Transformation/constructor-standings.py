# Databricks notebook source
dbutils.widgets.text('file_date','2021-03-21')
v_file_date = dbutils.widgets.get('file_date')

# COMMAND ----------

# MAGIC %run "../Includes/configurations"

# COMMAND ----------

# MAGIC %run "../Includes/common-functions"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder}/race_results").filter(f"file_date = '{v_file_date}'") 

# COMMAND ----------

race_years = columnToList(race_results_df, 'race_year')

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder}/race_results") \
.filter(col("race_year").isin(race_years))


# COMMAND ----------

constructor_standings_df = race_results_df \
.groupBy("race_year", "team") \
.agg(sum("points").alias("total_points"),
     count(when(col("position") == 1, True)).alias("wins"))


# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank, asc

constructor_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = constructor_standings_df.withColumn("rank", rank().over(constructor_rank_spec))


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Write data to presentation container**

# COMMAND ----------

overwritePartition(final_df, 'presentation_f1', 'constructor_standings', 'race_year')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select race_id,count(race_id)
# MAGIC
# MAGIC from presentation_f1.race_results
# MAGIC group by race_id
# MAGIC order by race_id desc