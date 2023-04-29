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

from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DoubleType
schema = StructType(fields=[StructField('circuitId',IntegerType(),nullable=False),
                            StructField('circuitRef',StringType(),True),
                            StructField('name',StringType(),True),
                            StructField('location',StringType(),True),
                            StructField('country',StringType(),True),
                            StructField('lat',DoubleType(),True),
                            StructField('lng',DoubleType(),True),
                            StructField('alt',IntegerType(),True),
                            StructField('url',StringType(),True),
])

# COMMAND ----------

# MAGIC %md
# MAGIC **read CSV**

# COMMAND ----------

circuits_df = spark.read.schema(schema).csv(f'{raw_folder}/{v_file_date}/circuits.csv',header=True)

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC **Select the required Columns**

# COMMAND ----------

from pyspark.sql.functions import col,lit

# COMMAND ----------

circuits_selected_df=circuits_df.select(col('circuitId'),col('circuitRef'),col('name'),col('location'),col('country'),col('lat'),col('lng'),col('alt'))

# COMMAND ----------

# MAGIC %md
# MAGIC **rename the columns**

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed('circuitId','circuit_id')\
    .withColumnRenamed('circuitRef','circuit_ref')\
    .withColumnRenamed('lat','latitude')\
    .withColumnRenamed('lng','longitude')\
    .withColumnRenamed('alt','altitude')\
        .withColumn('data_source',lit(v_data_source))\
            .withColumn('file_date',lit(v_file_date))


# COMMAND ----------

# MAGIC %md
# MAGIC **Adding ingestion date column**

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
circuits_final_df=addIngestionDate(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Write the final dataframe to processed folder as parquet**

# COMMAND ----------

circuits_final_df.write.mode("overwrite").format("delta").saveAsTable("processed_f1.circuits")

# COMMAND ----------

dbutils.notebook.exit('Success')