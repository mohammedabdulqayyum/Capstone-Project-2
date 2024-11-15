# Databricks notebook source
# MAGIC  %run /Workspace/Users/mabdulqayyum2002@gmail.com/Capstone-Project-2/Environment
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
def process_and_write_stream(df, name):
    df_with_ingestion_date = df.withColumn("ingestion_date", current_timestamp())
    df_non_null = df_with_ingestion_date.dropna(how='all')
    (df_non_null.writeStream
     .format("delta")
     .option("checkpointLocation", f"dbfs:/FileStore/checkpoints2_{name}")
     .outputMode("append")
     .trigger(availableNow=True)
     .table(f"capstone_project_2.bronze.{name}"))

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/adlssonydatabricks/raw/project2

# COMMAND ----------

dbutils.fs.rm("/FileStore/checkpoints2_results", True)  

# COMMAND ----------

df_driver = (spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.schemaLocation", "dbfs:/FileStore/schemas_capstone2driver")
      .option("cloudFiles.inferColumnTypes","true")
      .option("cloudFiles.schemaHints","number INTEGER")
      .load("dbfs:/mnt/adlssonydatabricks/raw/project2/drivers/"))
df_driver = df_driver.withColumnRenamed("driverId", "driver_id").withColumnRenamed("driverRef", "driver_ref").drop("url")
process_and_write_stream(df_driver, "drivers")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from capstone_project_2.bronze.driver

# COMMAND ----------

df_circuits = (spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .option("cloudFiles.schemaLocation", "dbfs:/FileStore/schemas_capstone2circuits")
      .option("header","true")
      .option("cloudFiles.inferColumnTypes","true")
      .load("dbfs:/mnt/adlssonydatabricks/raw/project2/circuits/"))

df_circuits = df_circuits.withColumn("circuitid", col("circuitid").cast("integer")).withColumn("lat", col("lat").cast("double")).withColumn("lng", col("lng").cast("double")).withColumn("alt", col("alt").cast("integer")).withColumnRenamed("circuitid", "circuit_id").withColumnRenamed("circuitRef", "circuit_ref").withColumnRenamed("lat", "latitude").withColumnRenamed("lng", "longitude").withColumnRenamed("alt", "altitude").drop("url")

process_and_write_stream(df_circuits, "circuits")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from capstone_project_2.bronze.circuits

# COMMAND ----------

from pyspark.sql.functions import concat, col, lit, to_timestamp

df_races = (spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .option("cloudFiles.schemaLocation", "dbfs:/FileStore/schemas_capstone2races")
      .option("header","true")
      .option("cloudFiles.inferColumnTypes","true")
      .load("dbfs:/mnt/adlssonydatabricks/raw/project2/races/"))

df_races = df_races.withColumn("race_timestamp", to_timestamp(concat(col("date"), lit(" "), col("time")))).withColumnRenamed("raceId", "race_id").withColumnRenamed("year", "race_year").withColumnRenamed("circuitId", "circuit_id").drop("url")

process_and_write_stream(df_races, "races")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from capstone_project_2.bronze.races

# COMMAND ----------

df_cons = (spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.schemaLocation", "dbfs:/FileStore/schemas_capstone2constructors")
      .option("cloudFiles.inferColumnTypes","true")
      .load("dbfs:/mnt/adlssonydatabricks/raw/project2/constructors/"))

df_cons = df_cons.withColumnRenamed("constructorId", "constructor_id").withColumnRenamed("constructorRef", "constructor_ref").drop("url")

process_and_write_stream(df_cons, "constructors")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from capstone_project_2.bronze.constructors

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table capstone_project_2.bronze.results

# COMMAND ----------

from pyspark.sql.functions import col

df_results = (spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.schemaLocation", "dbfs:/FileStore/schemas_capstone2results")
      .option("cloudFiles.inferColumnTypes","true")
      .option("cloudFiles.schemaHints", "fastestLap INTEGER, fastestLapSpeed DOUBLE, fastestLapTime STRING, milliseconds INTEGER, rank INTEGER")
      .load("dbfs:/mnt/adlssonydatabricks/raw/project2/results/"))


df_results = df_results.withColumnRenamed("constructorId", "constructor_id").withColumnRenamed("driverId", "driver_id").withColumnRenamed("raceId", "race_id").withColumnRenamed("resultId", "result_id").withColumnRenamed("positionText", "position_text").withColumnRenamed("positionOrder", "position_order").withColumnRenamed("fastestLap", "fastest_lap").withColumnRenamed("fastestLapSpeed", "fastest_lap_speed").withColumnRenamed("fastestLapTime", "fastest_lap_time").drop("statusId")

process_and_write_stream(df_results, "results")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from capstone_project_2.bronze.results

# COMMAND ----------

df_pitstops = (spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.schemaLocation", "dbfs:/FileStore/schemas_capstone2pitstops")
      .option("cloudFiles.inferColumnTypes","true")
      .option("cloudFiles.schemaHints", "duration DOUBLE, time TIMESTAMP")
      .option("multiline", "true")
      .load("dbfs:/mnt/adlssonydatabricks/raw/project2/pitstops/"))

df_pitstops = df_pitstops.withColumnRenamed("driverId", "driver_id").withColumnRenamed("raceId", "race_id")

process_and_write_stream(df_pitstops, "pitstops")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from capstone_project_2.bronze.pitstops

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, TimestampType, StringType

schema = StructType([
    StructField("raceId", IntegerType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("lap", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("milliseconds", DoubleType(), True)
])

df_laptime = (spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .option("cloudFiles.schemaLocation", "dbfs:/FileStore/schemas_capstone2laptimes")
      .schema(schema)
      .load("dbfs:/mnt/adlssonydatabricks/raw/project2/laptimes/lap_times/"))

df_laptime = df_laptime.withColumnRenamed("driverId", "driver_id").withColumnRenamed("raceId", "race_id")

process_and_write_stream(df_laptime, "laptimes")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from capstone_project_2.bronze.laptimes

# COMMAND ----------

df_qualifying = (spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.schemaLocation", "dbfs:/FileStore/schemas_capstone2qualifying")
      .option("cloudFiles.schemaHints", "driverId INTEGER, raceId INTEGER, constructorId INTEGER, number INTEGER, position INTEGER, qualifyId INTEGER")
      .option("multiline", "true")
      .load("dbfs:/mnt/adlssonydatabricks/raw/project2/qualifying/qualifying"))

df_qualifying = df_qualifying.withColumnRenamed("driverId", "driver_id").withColumnRenamed("raceId", "race_id").withColumnRenamed("constructorId", "constructor_id").withColumnRenamed("qualifyId", "qualifying_id")

process_and_write_stream(df_qualifying, "qualifying")

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists capstone_project_2.bronze.qualifying

# COMMAND ----------

# MAGIC %sql
# MAGIC select  * from capstone_project_2.bronze.qualifying

# COMMAND ----------


