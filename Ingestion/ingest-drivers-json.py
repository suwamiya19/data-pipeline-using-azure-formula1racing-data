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

from pyspark.sql.types import IntegerType,StringType,DateType,StructField,StructType

name_schema = StructType(fields = [StructField('forename',StringType(),False),
                                StructField('surname',StringType(),False)
                        ])


schema = StructType(fields = [StructField('driverId',IntegerType(),False),
                                StructField('driverRef',StringType(),False),
                                StructField('number',IntegerType(),True),
                                StructField('code',StringType(),True),
                                StructField('name',name_schema),
                                StructField('dob',DateType(),True),
                                StructField('nationality',StringType(),False),
                                StructField('url',StringType(),False),
                        ])

# COMMAND ----------

 drivers_df= spark.read.schema(schema).json(f'{raw_folder}/{v_file_date}/drivers.json')

# COMMAND ----------

from pyspark.sql.functions import col,concat,lit,current_timestamp

drivers_renamed_df=drivers_df.withColumnRenamed('driverId','driver_id')\
                            .withColumnRenamed('driverRef','driver_ref')\
                                .withColumn('name',concat(col('name.forename'),lit(' '),col('name.surname')))\
                                    .withColumn('data_source',lit(v_data_source))\
                                    .withColumn('file_date',lit(v_file_date))


drivers_renamed_df=addIngestionDate(drivers_renamed_df)

# COMMAND ----------

drivers_final_df=drivers_renamed_df.drop(col('url'))

# COMMAND ----------

display(drivers_final_df)

# COMMAND ----------

drivers_final_df.write.mode('overwrite').format("delta").saveAsTable("processed_f1.drivers")

# COMMAND ----------

dbutils.notebook.exit('Success')