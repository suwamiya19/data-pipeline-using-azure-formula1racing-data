-- Databricks notebook source
USE processed_f1

-- COMMAND ----------

CREATE TABLE presentation_f1.calculated_race_results
USING parquet
AS
SELECT races.race_year,
       constructors.name AS team_name,
       drivers.name AS driver_name,
       results.position,
       results.points,
       11 - results.position AS calculated_points
  FROM processed_f1.results 
  JOIN processed_f1.drivers ON (results.driver_id = drivers.driver_id)
  JOIN processed_f1.constructors ON (results.constructor_id = constructors.constructor_id)
  JOIN processed_f1.races ON (results.race_id = races.race_id)
 WHERE results.position <= 10

-- COMMAND ----------

SELECT * FROM presentation_f1.calculated_race_results