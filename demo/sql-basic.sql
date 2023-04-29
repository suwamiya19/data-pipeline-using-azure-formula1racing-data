-- Databricks notebook source
SHOW DATABASES

-- COMMAND ----------

SELECT current_database()

-- COMMAND ----------

USE processed_f1

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

DESC EXTENDED drivers

-- COMMAND ----------

SELECT * FROM drivers

-- COMMAND ----------

SELECT driver_id,name,nationality,dob 
FROM drivers
WHERE nationality = 'British'
AND dob > '1980-01-01'
ORDER BY dob DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ###SQL Functions

-- COMMAND ----------

SELECT concat( driver_id,'-',code ) as DriverCode

FROM drivers

-- COMMAND ----------

SELECT SPLIT(NAME,' ')[0] as FirstName, SPLIT(NAME,' ')[1] as LastName
FROM drivers

-- COMMAND ----------

SELECT current_timestamp(), driver_id

FROM drivers

-- COMMAND ----------

SELECT date_format(dob, 'dd-MM-yyyy' ),name
from drivers

-- COMMAND ----------

SELECT date_add(Ingestion_Date, 1), Ingestion_Date
from drivers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ###Aggregate Functions

-- COMMAND ----------

SELECT count(1)
from drivers

-- COMMAND ----------

SELECT MAX(DOB)
FROM DRIVERS
where nationality = 'Indian'

-- COMMAND ----------

SELECT nationality,count(driver_id) AS COUNT,MAX(DOB) AS ELDEST
FROM DRIVERS
GROUP BY nationality
HAVING COUNT(driver_id)>100
ORDER BY ELDEST

-- COMMAND ----------

SELECT driver_id,dob,NAME,nationality,rank() OVER (PARTITION BY nationality ORDER BY DOB DESC) AS AGERANK
FROM DRIVERS
ORDER BY nationality,AGERANK


-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ###SQL JOINS

-- COMMAND ----------

USE presentation_f1

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

