# Databricks notebook source

from pyspark.sql.functions import concat_ws, col, when

df_s_driver = spark.table("capstone_project_2.bronze.drivers")
df_s_driver = df_s_driver.withColumn("name", concat_ws(" ", col("name.forename"), col("name.surname"))).drop("_rescued_data").dropDuplicates().dropna()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS capstone_project_2.silver.driver_scd2 (
# MAGIC   driver_id BIGINT,
# MAGIC   driver_ref STRING,
# MAGIC   name STRING,
# MAGIC   dob STRING,
# MAGIC   nationality STRING,
# MAGIC   number BIGINT,
# MAGIC   ingestion_date TIMESTAMP,
# MAGIC   is_current BOOLEAN,
# MAGIC   start_date TIMESTAMP,
# MAGIC   end_date TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

from delta.tables import DeltaTable
 
df_s_driver = df_s_driver.dropDuplicates().dropna(how="all")
silver_table = DeltaTable.forName(spark, "capstone_project_2.silver.driver_scd2")
 
# Define the merge condition and update/insert actions
merge_condition = "silver.driver_id = bronze.driver_id AND silver.is_current = true"
update_action = {
    "is_current": "false",
    "end_date": "current_timestamp()"
}
insert_action = {
    "driver_id": "bronze.driver_id",
    "driver_ref": "bronze.driver_ref",
    "name": "bronze.name",
    "dob": "bronze.dob",
    "nationality": "bronze.nationality",
    "number": "bronze.number",
    "ingestion_date": "bronze.ingestion_date",
    "is_current": "true",
    "start_date": "current_timestamp()",
    "end_date": "null"
}
 
# Perform the merge
silver_table.alias("silver").merge(
    df_s_driver.alias("bronze"),
    merge_condition
).whenMatchedUpdate(
    set=update_action
).whenNotMatchedInsert(
    values=insert_action
).execute()
 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from capstone_project_2.silver.driver_scd2

# COMMAND ----------

df_s_circuits = spark.table("capstone_project_2.bronze.circuits")
df_s_circuits = df_s_circuits.drop("_rescued_data")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS capstone_project_2.silver.circuit_scd2 (
# MAGIC   circuit_id BIGINT,
# MAGIC   circuit_ref STRING,
# MAGIC   name STRING,
# MAGIC   location STRING,
# MAGIC   country STRING,
# MAGIC   latitude DOUBLE,
# MAGIC   longitude DOUBLE,
# MAGIC   altitude DOUBLE,
# MAGIC   ingestion_date TIMESTAMP,
# MAGIC   is_current BOOLEAN,
# MAGIC   start_date TIMESTAMP,
# MAGIC   end_date TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

from delta.tables import DeltaTable

df_s_circuits = df_s_circuits.dropDuplicates().dropna(how="all")
silver_table_circuits = DeltaTable.forName(spark, "capstone_project_2.silver.circuit_scd2")

# Define the merge condition and update/insert actions
merge_condition_circuits = "silver.circuit_id = bronze.circuit_id AND silver.is_current = true"
update_action_circuits = {
    "is_current": "false",
    "end_date": "current_timestamp()"
}
insert_action_circuits = {
    "circuit_id": "bronze.circuit_id",
    "circuit_ref": "bronze.circuit_ref",
    "name": "bronze.name",
    "location": "bronze.location",
    "country": "bronze.country",
    "latitude": "bronze.latitude",
    "longitude": "bronze.longitude",
    "altitude": "bronze.altitude",
    "ingestion_date": "bronze.ingestion_date",
    "is_current": "true",
    "start_date": "current_timestamp()",
    "end_date": "null"
}

# Perform the merge
silver_table_circuits.alias("silver").merge(
    df_s_circuits.alias("bronze"),
    merge_condition_circuits
).whenMatchedUpdate(
    set=update_action_circuits
).whenNotMatchedInsert(
    values=insert_action_circuits
).execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from capstone_project_2.silver.circuit_scd2

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table capstone_project_2.silver.constructor_scd2

# COMMAND ----------

df_s_constructors = spark.table("capstone_project_2.bronze.constructors")
df_s_constructors = df_s_constructors.drop("_rescued_data")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS capstone_project_2.silver.constructor_scd2 (
# MAGIC   constructor_id BIGINT,
# MAGIC   constructor_ref STRING,
# MAGIC   name STRING,
# MAGIC   nationality STRING,
# MAGIC   ingestion_date TIMESTAMP,
# MAGIC   is_current BOOLEAN,
# MAGIC   start_date TIMESTAMP,
# MAGIC   end_date TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

from delta.tables import DeltaTable

df_s_constructors = df_s_constructors.dropDuplicates().dropna(how="all")
silver_table_constructors = DeltaTable.forName(spark, "capstone_project_2.silver.constructor_scd2")

# Define the merge condition and update/insert actions
merge_condition_constructors = "silver.constructor_id = bronze.constructor_id AND silver.is_current = true"
update_action_constructors = {
    "is_current": "false",
    "end_date": "current_timestamp()"
}
insert_action_constructors = {
    "constructor_id": "bronze.constructor_id",
    "constructor_ref": "bronze.constructor_ref",
    "name": "bronze.name",
    "nationality": "bronze.nationality",
    "ingestion_date": "bronze.ingestion_date",
    "is_current": "true",
    "start_date": "current_timestamp()",
    # Use "null" as a string for null values in SQL
    "end_date": "null"
}

# Perform the merge
silver_table_constructors.alias("silver").merge(
    df_s_constructors.alias("bronze"),
    merge_condition_constructors
).whenMatchedUpdate(
    set=update_action_constructors
).whenNotMatchedInsert(
    values=insert_action_constructors
).execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from capstone_project_2.silver.constructor_scd2

# COMMAND ----------

df_s_pitstops = spark.table("capstone_project_2.bronze.pitstops")
df_s_pitstops = df_s_pitstops.drop("_rescued_data").dropDuplicates().dropna(how="all")
df_s_pitstops.write.mode("overwrite").saveAsTable("capstone_project_2.silver.pitstop")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from capstone_project_2.silver.pitstop

# COMMAND ----------

df_s_laptimes = spark.table("capstone_project_2.bronze.laptimes")
df_s_laptimes = df_s_laptimes.drop("_rescued_data").dropDuplicates().dropna(how="all")
df_s_laptimes.write.mode("overwrite").saveAsTable("capstone_project_2.silver.laptimes")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from capstone_project_2.silver.laptimes

# COMMAND ----------

from pyspark.sql.functions import col

df_s_qualifying = spark.table("capstone_project_2.bronze.qualifying")
df_s_qualifying = df_s_qualifying.drop("_rescued_data").replace("\\N", None).dropna().dropDuplicates()
df_s_qualifying.write.mode("overwrite").saveAsTable("capstone_project_2.silver.qualifying")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from capstone_project_2.silver.qualifying

# COMMAND ----------

df_s_races = spark.table("capstone_project_2.bronze.races")
df_s_races = df_s_races.drop("_rescued_data", "date", "time").dropna().dropDuplicates()
df_s_races.write.mode("overwrite").partitionBy("race_year").saveAsTable("capstone_project_2.silver.races")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from capstone_project_2.silver.races

# COMMAND ----------

df_s_results = spark.table("capstone_project_2.bronze.results")
df_s_results = df_s_results.drop("_rescued_data")
df_s_results = df_s_results.replace("\\N", None).dropDuplicates().dropna()
df_s_results.write.mode("overwrite").partitionBy("race_id").saveAsTable("capstone_project_2.silver.results")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from capstone_project_2.silver.results
