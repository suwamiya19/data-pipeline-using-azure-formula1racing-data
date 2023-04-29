# Databricks notebook source
dbutils.widgets.text('file_date','2021-03-21')
v_file_date = dbutils.widgets.get('file_date')

# COMMAND ----------

# MAGIC %run "../Includes/configurations"

# COMMAND ----------

# MAGIC %run "../Includes/common-functions"

# COMMAND ----------

# MAGIC %md
# MAGIC **Read data from processed container for drivers, constructors, circuits, races, results**

# COMMAND ----------

drivers_df = spark.read.parquet(f"{processed_folder}/drivers") \
.withColumnRenamed("number", "driver_number") \
.withColumnRenamed("name", "driver_name") \
.withColumnRenamed("nationality", "driver_nationality") 

# COMMAND ----------

constructors_df = spark.read.parquet(f"{processed_folder}/constructors") \
.withColumnRenamed("name", "team") 

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder}/circuits") \
.withColumnRenamed("location", "circuit_location") 

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder}/races") \
.withColumnRenamed("name", "race_name") \
.withColumnRenamed("race_timestamp", "race_date")

# COMMAND ----------

results_df = spark.read.parquet(f"{processed_folder}/results") \
.filter(f'file_date = "{v_file_date}"')\
    .withColumnRenamed("time", "race_time")\
        .withColumnRenamed("race_id","result_race_id")\
            .withColumnRenamed("file_date","result_file_date")

# COMMAND ----------

v_file_date

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Join races and circuits** 

# COMMAND ----------

races_circuits_df = races_df.join(circuits_df,races_df.circuit_id == circuits_df.circuit_id, "inner")\
    .select(races_df.race_id, races_df.race_year, races_df.race_name, races_df.race_date, circuits_df.circuit_location)


# COMMAND ----------

# MAGIC %md
# MAGIC **Join all dataframes**

# COMMAND ----------

race_results_df = results_df.join(races_circuits_df, results_df.result_race_id == races_circuits_df.race_id) \
                            .join(drivers_df, results_df.driver_id == drivers_df.driver_id) \
                            .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id)


# COMMAND ----------

from pyspark.sql.functions import current_timestamp

final_df = race_results_df.select("race_id","race_year", "race_name", "race_date", "circuit_location", "driver_name", "driver_number", "driver_nationality",
                                 "team", "grid", "fastest_lap", "race_time", "points", "position","result_file_date") \
                                    .withColumn("created_date", current_timestamp())\
                                        .withColumnRenamed("result_file_date","file_date")

# COMMAND ----------

display(final_df.filter("race_year == 2020 and race_name == 'Abu Dhabi Grand Prix'").orderBy(final_df.points.desc()))


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Write the result to presentation to container**

# COMMAND ----------

overwritePartition(final_df,'presentation_f1','race_results','race_id')

# COMMAND ----------

display(spark.read.parquet(f"{presentation_folder}/race_results"))

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select race_id,count(race_id)
# MAGIC
# MAGIC from presentation_f1.race_results
# MAGIC group by race_id
# MAGIC order by race_id desc

# COMMAND ----------

