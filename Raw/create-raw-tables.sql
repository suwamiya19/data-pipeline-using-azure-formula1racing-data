-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS raw_f1;

-- COMMAND ----------

DROP TABLE IF EXISTS raw_f1.circuits;
CREATE TABLE IF NOT EXISTS raw_f1.circuits(circuitId INT,
circuitRef STRING,
name STRING,
location STRING,
country STRING,
lat DOUBLE,
lng DOUBLE,
alt INT,
url STRING
)
USING csv
OPTIONS (path "/mnt/projformula1dl/raw/circuits.csv", header true)

-- COMMAND ----------

SELECT * FROM raw_f1.circuits;

-- COMMAND ----------

DROP TABLE IF EXISTS raw_f1.races;
CREATE TABLE IF NOT EXISTS raw_f1.races(raceId INT,
year INT,
round INT,
circuitId INT,
name STRING,
date DATE,
time STRING,
url STRING)
USING csv
OPTIONS (path "/mnt/projformula1dl/raw/races.csv", header true)

-- COMMAND ----------

SELECT * FROM raw_f1.races;

-- COMMAND ----------

DROP TABLE IF EXISTS raw_f1.constructors;
CREATE TABLE IF NOT EXISTS raw_f1.constructors(
constructorId INT,
constructorRef STRING,
name STRING,
nationality STRING,
url STRING)
USING json
OPTIONS(path "/mnt/projformula1dl/raw/constructors.json")


-- COMMAND ----------

SELECT * FROM raw_f1.constructors;

-- COMMAND ----------

DROP TABLE IF EXISTS raw_f1.drivers;
CREATE TABLE IF NOT EXISTS raw_f1.drivers(
driverId INT,
driverRef STRING,
number INT,
code STRING,
name STRUCT<forename: STRING, surname: STRING>,
dob DATE,
nationality STRING,
url STRING)
USING json
OPTIONS (path "/mnt/projformula1dl/raw/drivers.json")

-- COMMAND ----------

SELECT * FROM raw_f1.drivers;

-- COMMAND ----------

DROP TABLE IF EXISTS raw_f1.results;
CREATE TABLE IF NOT EXISTS raw_f1.results(
resultId INT,
raceId INT,
driverId INT,
constructorId INT,
number INT,grid INT,
position INT,
positionText STRING,
positionOrder INT,
points INT,
laps INT,
time STRING,
milliseconds INT,
fastestLap INT,
rank INT,
fastestLapTime STRING,
fastestLapSpeed FLOAT,
statusId STRING)
USING json
OPTIONS(path "/mnt/projformula1dl/raw/results.json")


-- COMMAND ----------

SELECT * FROM raw_f1.results;

-- COMMAND ----------

DROP TABLE IF EXISTS raw_f1.pit_stops;
CREATE TABLE IF NOT EXISTS raw_f1.pit_stops(
driverId INT,
duration STRING,
lap INT,
milliseconds INT,
raceId INT,
stop INT,
time STRING)
USING json
OPTIONS(path "/mnt/projformula1dl/raw/pit_stops.json", multiLine true)

-- COMMAND ----------

SELECT * FROM raw_f1.pit_stops;

-- COMMAND ----------

DROP TABLE IF EXISTS raw_f1.lap_times;
CREATE TABLE IF NOT EXISTS raw_f1.lap_times(
raceId INT,
driverId INT,
lap INT,
position INT,
time STRING,
milliseconds INT
)
USING csv
OPTIONS (path "/mnt/projformula1dl/raw/lap_times")

-- COMMAND ----------

SELECT * FROM raw_f1.lap_times

-- COMMAND ----------

DROP TABLE IF EXISTS raw_f1.qualifying;
CREATE TABLE IF NOT EXISTS raw_f1.qualifying(
constructorId INT,
driverId INT,
number INT,
position INT,
q1 STRING,
q2 STRING,
q3 STRING,
qualifyId INT,
raceId INT)
USING json
OPTIONS (path "/mnt/projformula1dl/raw/qualifying", multiLine true)

-- COMMAND ----------

SELECT * FROM raw_f1.qualifying

-- COMMAND ----------

