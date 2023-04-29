# Databricks notebook source
# MAGIC %md
# MAGIC 1. Read csv file from raw
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
# MAGIC **Define Schema**

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DoubleType,DateType
schema = StructType(fields=[StructField('raceId',IntegerType(),nullable=False),
                            StructField('year',IntegerType(),True),
                            StructField('round',IntegerType(),True),
                            StructField('circuitId',IntegerType(),True),
                            StructField('name',StringType(),True),
                            StructField('date',DateType(),True),
                            StructField('time',StringType(),True),
                            StructField('url',IntegerType(),True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC **read CSV**

# COMMAND ----------

races_df = spark.read.schema(schema).csv(f'{raw_folder}/{v_file_date}/races.csv',header=True)

# COMMAND ----------

races_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC **Select the required Columns**

# COMMAND ----------

from pyspark.sql.functions import col,lit

# COMMAND ----------

races_selected_df=races_df.select(col('raceId'),col('year'),col('round'),col('circuitId'),col('name'),col('date'),col('time'))

# COMMAND ----------

# MAGIC %md
# MAGIC **rename the columns**

# COMMAND ----------

races_renamed_df = races_selected_df.withColumnRenamed('raceId','race_id')\
    .withColumnRenamed('year','race_year')\
    .withColumnRenamed('circuitId','circuit_id')\
        .withColumn("data_source", lit(v_data_source))\
            .withColumn('file_date',lit(v_file_date))
display(races_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Combing date and time to race_timestamp**

# COMMAND ----------

from pyspark.sql.functions import to_timestamp,concat,lit
races_timestamp_df=races_renamed_df.withColumn('race_timestamp',to_timestamp(concat(col('date'),lit(' '),col('time')),'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

# MAGIC %md
# MAGIC **Adding ingestion date column**

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
races_final_df=addIngestionDate(races_timestamp_df)

# COMMAND ----------

races_final_df = races_final_df.select(col('race_id'),col('race_year'),col('round'),col('circuit_id'),col('name'),col('race_timestamp'),col('Ingestion_date'))
display(races_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Write the final dataframe to processed folder as parquet**

# COMMAND ----------

races_final_df.write.mode('overwrite').partitionBy('race_year').format("delta").saveAsTable("processed_f1.races")

# COMMAND ----------

dbutils.notebook.exit('Success')