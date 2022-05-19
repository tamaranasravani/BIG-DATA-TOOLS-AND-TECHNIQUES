-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC ## Overview
-- MAGIC 
-- MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
-- MAGIC 
-- MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls ("dbfs:/user/hive/warehouse/df2019_clin_table")

-- COMMAND ----------

-- DBTITLE 1,DATASET INGESTION TO DATAFRAME
-- MAGIC %python
-- MAGIC files_2019 = ["/FileStore/tables/clinicaltrial_2019_csv.gz"]
-- MAGIC files_2020 = ["/FileStore/tables/clinicaltrial_2020_csv.gz"]
-- MAGIC files_2021 = ["/FileStore/tables/clinicaltrial_2021_csv.gz"]
-- MAGIC mesh_file_name = "/FileStore/tables/mesh.csv"
-- MAGIC pharma_file_name = "/FileStore/tables/pharma.csv"
-- MAGIC 
-- MAGIC 
-- MAGIC df_2019 = spark.read.options(header='True', inferSchema='True', delimiter='|').csv(files_2019)
-- MAGIC df_2020 = spark.read.options(header='True', inferSchema='True', delimiter='|').csv(files_2020)
-- MAGIC df_2021 = spark.read.options(header='True', inferSchema='True', delimiter='|').csv(files_2021)
-- MAGIC file_name = "/FileStore/tables/mesh.csv"
-- MAGIC delimiter = ","
-- MAGIC 
-- MAGIC 
-- MAGIC meshdf = spark.read.format(file_type) \
-- MAGIC   .option("inferSchema", infer_schema) \
-- MAGIC   .option("header", first_row_is_header) \
-- MAGIC   .option("sep", delimiter) \
-- MAGIC   .load(file_name)
-- MAGIC 
-- MAGIC file_name = "/FileStore/tables/pharma.csv"
-- MAGIC df = spark.read.format(file_type) \
-- MAGIC   .option("inferSchema", infer_schema) \
-- MAGIC   .option("header", first_row_is_header) \
-- MAGIC   .option("sep", delimiter) \
-- MAGIC   .load(file_name)

-- COMMAND ----------

-- DBTITLE 1,DEFINING HIVE TABLES FROM EXISTING DATAFRAMES
-- MAGIC %python
-- MAGIC # creating hive tables from existing dataframes
-- MAGIC # Hive table Location : dbfs:/user/hive/warehouse/df_2019
-- MAGIC # Hive table Location : dbfs:/user/hive/warehouse/df_2020
-- MAGIC # Hive table Location : dbfs:/user/hive/warehouse/df_2021
-- MAGIC tableName_2019 = "clin_df2019_table"
-- MAGIC tableName_2020 = "clin_df2020_table"
-- MAGIC tableName_2021 = "clin_df2021_table"
-- MAGIC df_2019.write.saveAsTable(tableName_2019)
-- MAGIC df_2020.write.saveAsTable(tableName_2020)
-- MAGIC df_2021.write.saveAsTable(tableName_2021)

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC show create table clin_df2019_table

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC describe formatted clin_df2019_table

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC show create table clin_df2019_table

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC describe formatted clin_df2019_table

-- COMMAND ----------

-- DBTITLE 1,HIVE TABLE DESIGN
-- MAGIC %sql
-- MAGIC --show create table df2021_table;
-- MAGIC show create table clin_df2019_table;

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC describe formatted clin_df2019_table

-- COMMAND ----------

-- DBTITLE 1,Hive table data validation
-- MAGIC %sql
-- MAGIC -- Hive Data Validation
-- MAGIC 
-- MAGIC SELECT distinct COUNT(ID),2019 as year FROM clin_df2019_table union SELECT distinct COUNT(ID),2020 as year FROM clin_df2020_table union SELECT distinct COUNT(ID),2021 as year FROM clin_df2021_table

-- COMMAND ----------

-- DBTITLE 1,QUESTION -1 HIVE 2019 2020 2021
-- MAGIC %sql
-- MAGIC -- QUESTION -1 The number of studies in the dataset
-- MAGIC --Hive
-- MAGIC 
-- MAGIC SELECT distinct COUNT(ID),2019 as year FROM clin_df2019_table union SELECT distinct COUNT(ID),2020 as year FROM clin_df2020_table union SELECT distinct COUNT(ID),2021 as year FROM clin_df2021_table

-- COMMAND ----------

-- DBTITLE 1,QUESTION -2 HIVE 2019
-- MAGIC %sql
-- MAGIC 
-- MAGIC SELECT DISTINCT TYPE, COUNT(*) FROM clin_df2019_table GROUP BY Type order by 1 ASC 

-- COMMAND ----------

-- DBTITLE 1,QUESTION -2 HIVEE 2020
-- MAGIC %sql
-- MAGIC --Hive
-- MAGIC SELECT DISTINCT TYPE, COUNT(*) FROM clin_df2020_table GROUP BY Type order by 1 ASC 

-- COMMAND ----------

-- DBTITLE 1,QUESTION -2 HIVEE 2021
-- MAGIC %sql
-- MAGIC --Hive
-- MAGIC SELECT DISTINCT TYPE, COUNT(*) FROM clin_df2021_table GROUP BY Type order by 1 ASC 

-- COMMAND ----------

-- DBTITLE 1,QUESTION -3 HIVE 2019
-- MAGIC %sql
-- MAGIC with t as (
-- MAGIC select explode(split(Conditions,"\\,")) as conditions from clin_df2019_table
-- MAGIC )
-- MAGIC select count(*), conditions from t group by conditions order by 1 desc

-- COMMAND ----------

-- DBTITLE 1,QUESTION -3 HIVE 2020
-- MAGIC %sql
-- MAGIC with t as (
-- MAGIC select explode(split(Conditions,"\\,")) as conditions from clin_df2020_table
-- MAGIC )
-- MAGIC select count(*), conditions from t group by conditions order by 1 desc

-- COMMAND ----------

-- DBTITLE 1,QUESTION -3 HIVE 2021
-- MAGIC %sql
-- MAGIC with t as (
-- MAGIC select explode(split(Conditions,"\\,")) as conditions from clin_df2021_table
-- MAGIC )
-- MAGIC select count(*), conditions from t group by conditions order by 1 desc

-- COMMAND ----------

-- DBTITLE 1,QUESTION -4 CREATING HIVE TABLE FROM PYSPARK DATAFRAME
-- MAGIC %python
-- MAGIC tableName = "meshtable_clin"
-- MAGIC meshdf.write.saveAsTable(tableName)
-- MAGIC 
-- MAGIC #df_2019 = spark.read.options(header='True', inferSchema='True', delimiter='|') .csv(files_2019)

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC show create table meshtable_clin

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC describe formatted meshtable_clin

-- COMMAND ----------

-- DBTITLE 1,QUESTION -4 HIVE 2019
-- MAGIC %sql
-- MAGIC 
-- MAGIC with conditions_data as(
-- MAGIC select explode(split(Conditions,"\\,")) as conditions from clin_df2019_table),
-- MAGIC --select * from conditions_data
-- MAGIC mesh_data as (
-- MAGIC select term,tree,substring(tree,1,3) as root from meshtable_clin
-- MAGIC ),
-- MAGIC --select * from mesh_data
-- MAGIC join_data as(
-- MAGIC select Conditions,root from (
-- MAGIC   SELECT dt.Conditions, pt.term,pt.root FROM conditions_data dt LEFT OUTER JOIN mesh_data pt ON (dt.Conditions==pt.term)
-- MAGIC ) 
-- MAGIC )
-- MAGIC select root,count(*) from join_data where root is not null group  by root order by 2 desc

-- COMMAND ----------

-- DBTITLE 1,QUESTION -4 HIVE 2020
-- MAGIC %sql
-- MAGIC with conditions_data as(
-- MAGIC    select explode(split(Conditions,"\\,")) as conditions from clin_df2020_table
-- MAGIC                         ),
-- MAGIC     --select * from conditions_data
-- MAGIC mesh_data as (
-- MAGIC    select term,tree,substring(tree,1,3) as root from meshtable_clin
-- MAGIC              ),
-- MAGIC --select * from mesh_data
-- MAGIC join_data as(
-- MAGIC     select Conditions,root from (
-- MAGIC          SELECT dt.Conditions, pt.term,pt.root FROM conditions_data dt LEFT OUTER JOIN mesh_data pt ON (dt.Conditions==pt.term)
-- MAGIC                                 ) 
-- MAGIC             )
-- MAGIC select root,count(*) from join_data where root is not null group  by root order by 2 desc

-- COMMAND ----------

-- DBTITLE 1,QUESTION -4 HIVE 2021
-- MAGIC %sql
-- MAGIC with conditions_data as(
-- MAGIC    select explode(split(Conditions,"\\,")) as conditions from clin_df2021_table
-- MAGIC                         ),
-- MAGIC     --select * from conditions_data
-- MAGIC mesh_data as (
-- MAGIC    select term,tree,substring(tree,1,3) as root from meshtable_clin
-- MAGIC              ),
-- MAGIC --select * from mesh_data
-- MAGIC join_data as(
-- MAGIC     select Conditions,root from (
-- MAGIC          SELECT dt.Conditions, pt.term,pt.root FROM conditions_data dt LEFT OUTER JOIN mesh_data pt ON (dt.Conditions==pt.term)
-- MAGIC                                 ) 
-- MAGIC             )
-- MAGIC select root,count(*) from join_data where root is not null group  by root order by 2 desc

-- COMMAND ----------

-- DBTITLE 1,QUESTION 5 - HIVE - 
-- MAGIC %python
-- MAGIC # QUESTION 5 - HIVE
-- MAGIC # creating pharma table in hive 
-- MAGIC tableName = "pharmatable_clin"
-- MAGIC df.write.saveAsTable(tableName)

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC show create table pharmatable_clin

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC describe formatted pharmatable_clin

-- COMMAND ----------

-- DBTITLE 1,question 5 - HIVE 2019
-- MAGIC %sql
-- MAGIC with join_clinical_data_with_pharma as (
-- MAGIC SELECT dt.Sponsor, pt.Parent_Company FROM clin_df2019_table dt LEFT OUTER JOIN pharmatable pt ON (dt.Sponsor==pt.Parent_Company)
-- MAGIC where nvl(pt.Major_Industry_of_Parent,'lit')!= 'pharmaceuticals')
-- MAGIC _
-- MAGIC select count(*),Sponsor from join_clinical_data_with_pharma
-- MAGIC group by Sponsor order by 1 desc;

-- COMMAND ----------

-- DBTITLE 1,question 5 - HIVE 2020
-- MAGIC %sql
-- MAGIC with join_clinical_data_with_pharma as (
-- MAGIC SELECT dt.Sponsor, pt.Parent_Company FROM clin_df2020_table dt LEFT OUTER JOIN pharmatable_clin pt ON (dt.Sponsor==pt.Parent_Company)
-- MAGIC where nvl(pt.Major_Industry_of_Parent,'lit')!= 'pharmaceuticals')
-- MAGIC 
-- MAGIC select count(*),Sponsor from join_clinical_data_with_pharma
-- MAGIC group by Sponsor order by 1 desc

-- COMMAND ----------

-- DBTITLE 1,QUESTION -5 HIVE 2021
-- MAGIC %sql
-- MAGIC with join_clinical_data_with_pharma as (
-- MAGIC SELECT dt.Sponsor, pt.Parent_Company FROM clin_df2021_table dt LEFT OUTER JOIN pharmatable_clin pt ON (dt.Sponsor==pt.Parent_Company)
-- MAGIC where nvl(pt.Major_Industry_of_Parent,'lit')!= 'pharmaceuticals')
-- MAGIC 
-- MAGIC select count(*),Sponsor from join_clinical_data_with_pharma
-- MAGIC group by Sponsor order by 1 desc

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC show create table clin_df2019_table

-- COMMAND ----------

-- DBTITLE 1,QUESTION -6 HIVE 2019
-- MAGIC %sql
-- MAGIC with clinical_data as (
-- MAGIC select Completion,Status,date_format(to_date(Completion,"MMM yyyy"),"MM-dd-yyyy") as col from clin_df2019_table
-- MAGIC where Status = "Completed"),
-- MAGIC 
-- MAGIC clinical_data_2019 as (
-- MAGIC select * from clinical_data where substring(Completion,5,8)== "2019"),
-- MAGIC --select * from clinical_data_2019
-- MAGIC 
-- MAGIC yearwise_count as (
-- MAGIC select substring(Completion, 1,3) as month,Status,col from clinical_data_2019
-- MAGIC )
-- MAGIC --select * from yearwise_count
-- MAGIC 
-- MAGIC select count(*), month from yearwise_count group by month order by 1 desc

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC show create table clin_df2020_table

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC describe extended clin_df2020_table

-- COMMAND ----------

-- DBTITLE 1,QUESTION 6 HIVE 2020
-- MAGIC %sql
-- MAGIC with clinical_data as (
-- MAGIC select Completion,Status,date_format(to_date(Completion,"MMM yyyy"),"MM-dd-yyyy") as col from clin_df2020_table
-- MAGIC where Status = "Completed"),
-- MAGIC 
-- MAGIC clinical_data_2020 as (
-- MAGIC select * from clinical_data where substring(Completion,5,8)== "2020"),
-- MAGIC --select * from clinical_data_2020
-- MAGIC 
-- MAGIC yearwise_count as (
-- MAGIC select substring(Completion, 1,3) as month,Status,col from clinical_data_2020
-- MAGIC )
-- MAGIC --select * from yearwise_count
-- MAGIC 
-- MAGIC select count(*), month from yearwise_count group by month order by 1 desc

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC show create table clin_df2021_table

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC describe formatted clin_df2021_table

-- COMMAND ----------

-- DBTITLE 1,QUESTION -6 HIVE 2021
-- MAGIC %sql
-- MAGIC with clinical_data as (
-- MAGIC select Completion,Status,date_format(to_date(Completion,"MMM yyyy"),"MM-dd-yyyy") as col from clin_df2021_table
-- MAGIC where Status = "Completed"),
-- MAGIC 
-- MAGIC clinical_data_2021 as (
-- MAGIC select * from clinical_data where substring(Completion,5,8)== "2021"),
-- MAGIC --select * from clinical_data_2021
-- MAGIC 
-- MAGIC yearwise_count as (
-- MAGIC select substring(Completion, 1,3) as month,Status,col from clinical_data_2021
-- MAGIC )
-- MAGIC --select * from yearwise_count
-- MAGIC 
-- MAGIC select count(*), month from yearwise_count group by month order by 1 desc

-- COMMAND ----------


