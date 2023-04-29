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

schema = 'resultId INT, raceId INT, driverId INT, constructorId INT, number  INT, grid INT, position INT, positionText  STRING ,positionOrder INT ,points    FLOAT ,laps INT, time STRING, milliseconds  INT, fastestLap    INT,rank INT    ,fastestLapTime STRING,fastestLapSpeed STRING,statusId INT'

# COMMAND ----------

results_df = spark.read.schema(schema).json(f'{raw_folder}/{v_file_date}/results.json')

# COMMAND ----------

results_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,col,lit

results_renamed_df=results_df.withColumnRenamed('resultId','result_id')\
                            .withColumnRenamed('raceId','race_id')\
                                .withColumnRenamed('driverId','driver_id')\
                                    .withColumnRenamed('constructorId','constructor_id')\
                                        .withColumnRenamed('positionText','position_text')\
                                            .withColumnRenamed('positionOrder','position_order')\
                                                .withColumnRenamed('fastestLap','fastest_lap')\
                                                    .withColumnRenamed('fastestLapTime','fastest_lap_time')\
                                                        .withColumnRenamed('fastestLapSpeed','fastest_lap_speed')\
                                                            .withColumn('data_source',lit(v_data_source)) \
                                                                .withColumn("file_date", lit(v_file_date))

results_renamed_df=addIngestionDate(results_renamed_df)                                                           

# COMMAND ----------

results_final_df=results_renamed_df.drop(col('statusId'))

# COMMAND ----------

display(results_final_df)

# COMMAND ----------

overwritePartition(results_final_df, 'processed_f1', 'results', 'race_id')

if (spark._jsparkSession.catalog().tableExists('processed_f1.results')):
    output_df.write.mode("overwrite").insertInto('processed_f1.results')
  else:
    output_df.write.mode("overwrite").partitionBy('race_id').format("delta").saveAsTable('processed_f1.results')


# COMMAND ----------

display(spark.read.parquet(f'{processed_folder}/results'))

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/projformula1dl/processed/results

# COMMAND ----------

dbutils.notebook.exit('Success')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC race_id,count(1) 
# MAGIC FROM processed_f1.results
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC