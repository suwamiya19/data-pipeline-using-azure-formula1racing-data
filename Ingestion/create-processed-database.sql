-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS processed_f1
LOCATION "/mnt/projformula1dl/processed"

-- COMMAND ----------

DESC DATABASE EXTENDED processed_f1;