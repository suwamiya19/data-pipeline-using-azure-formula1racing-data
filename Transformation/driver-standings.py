# Databricks notebook source
dbutils.widgets.text('file_date','2021-03-21')
v_file_date = dbutils.widgets.get('file_date')

# COMMAND ----------

# MAGIC %run "../Includes/configurations"

# COMMAND ----------

# MAGIC %run "../Includes/common-functions"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder}/race_results").filter(f'file_date="{v_file_date}"')

# COMMAND ----------

race_years = columnToList(race_results_df,'race_year')

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder}/race_results") \
.filter(col("race_year").isin(race_years))


# COMMAND ----------

driver_standings_df = race_results_df.groupBy("race_year", "driver_name", "driver_nationality", "team") \
                            .agg(sum("points").alias("total_points"),
                                count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

display(driver_standings_df.filter("race_year = 2020"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank, asc

# COMMAND ----------

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = driver_standings_df.withColumn("rank", rank().over(driver_rank_spec))

# COMMAND ----------

display(final_df.filter("race_year = 2020"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Write result to presenation container**

# COMMAND ----------

overwritePartition(final_df,"presentation_f1","driver_standings","race_year")

# COMMAND ----------

