-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC  *we are going to design our pipelines, we are going to have additional columns in our tables in process and presentation.*

-- COMMAND ----------

DROP DATABASE IF EXISTS processed_F1 CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS processed_f1
LOCATION "/mnt/projformula1dl/processed";

-- COMMAND ----------

DROP DATABASE IF EXISTS presentation_f1 CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS presentation_f1 
LOCATION "/mnt/projformula1dl/presentation";