# formula1-data-analysis-using-azure-databricks
Analysing Formula 1 data using Azure Databricks and ADF

## Summary

Formula1 is a once-a-year international motor racing competition. It typically holds 20 races, each of which takes place over the weekend. The constructor teams, which will make up about 10 of the teams in the league, will compete. There will be two drivers per constructor. Depending on the circuit, each race comprises 50 to 70 laps.

We have analyzed the drivers champion and constructor champion by loading the data from a third party API called Ergast to **Azure Data Lake Gen 2 storage**. Data transformation and analysis are done in **Azure Databricks**. The entire process is orchestrated by **Azure Data Factory**

## Tools & Technologies Used:
1. Azure Data Lake Gen 2 Storage
2. Azure Data Bricks
3. Pyspark
4. SQL
5. Azure Data Factory

## Data Source
Data source is a third party API called Ergast, https://Ergast.com/mrd which contains from 1950. Following is the Entity Relationship Diagram from the API documentation
![image](https://user-images.githubusercontent.com/64007718/235310215-2b61f2d2-0756-41b2-a373-11dbb591245e.png)

## SOLUTION ARCHITECTURE:

The solution architecture is based on [Azure Modern Data Analytic Architecture](https://learn.microsoft.com/en-us/azure/architecture/solution-ideas/articles/azure-databricks-modern-analytics-architecture)

Ergast API -> ADLS Raw Layer -> Ingest (Azure Databricks) -> Processed layer (ADLS) -> Transform (Azure Databricks) -> Presentation layer (ADLS) -> Analyze (Azure Databricks) -> BI & Reporting

## Project Requirement:

Following are the requirements for the various stages in our solution,

- Ingestion:
	- Ingest all the 8 files which are of different formats into the data lake
	- The data ingested should have schema applied
	- The data should have audit columns such as ingested data and the source
	- The data should be stored in parquet format
	- The data should be available for ML, reporting and analytical workloads
	- Incremental load for data ingestion - data should be appended in data lake instead of replacing the data

- Transformation
	- Join the key information required for reporting to create a new table
	- Join the key information required for reporting to create a new table
	- The tables should have audit columns such as ingested data and the source
	- The tables should be available for ML, reporting and analytical workloads
	- Transformed data should be stored in parquet format
	- Transformation logic should support incremental workload
- Reporting:
	- Driver standings
	- Constructor standings
	- For current race year as well as every year from 1950

- Analysis:
	- Dominant Drivers
	- Dominant Teams
	- Visualize output
	- Create dashboard in databricks
- Scheduling
	- Pipeline to run every Sunday 10 PM
	- Ability to monitor pipeline
	- Re-run failed pipelines
	- Alerts for failures in pipeline
- Others:
	- Ability to delete individual records.
	- Other Non-Functional Requirements.
	- Ability to see history and time travel.
	- Ability to roll back to a previous version.

## Analysis Result:
![image](https://user-images.githubusercontent.com/64007718/235310453-95b6d253-aaab-454b-87f1-8fb722600014.png)
![image](https://user-images.githubusercontent.com/64007718/235310459-c9141816-2832-4be7-8902-3fce7096c88d.png)
![image](https://user-images.githubusercontent.com/64007718/235310466-4a83e4ce-00c3-444c-b22a-83ad42530321.png)
![image](https://user-images.githubusercontent.com/64007718/235310470-9c966e29-ba76-4c10-9554-f201d72ee636.png)
![image](https://user-images.githubusercontent.com/64007718/235310476-98db1649-0fb4-45f5-bfc4-8892afc8bc80.png)
![image](https://user-images.githubusercontent.com/64007718/235310486-98404d97-ed11-4be2-90c3-535f538cfdc9.png)
