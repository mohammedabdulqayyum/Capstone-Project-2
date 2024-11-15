# Databricks notebook source
df_races2 = spark.table("capstone_project_2.silver.races")
df_results2 = spark.table("capstone_project_2.silver.results")
df_drivers2 = spark.table("capstone_project_2.silver.driver_scd2")
df_constructors2 = spark.table("capstone_project_2.silver.constructor_scd2")

# COMMAND ----------

df_races2 = df_races2.withColumnRenamed("name", "race_name").drop("ingestion_date")
display(df_races2)

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

df_drivers2 = df_drivers2.withColumnRenamed("name", "driver_name").drop("ingestion_date","is_current","start_date","end_date")
display(df_drivers2)

# COMMAND ----------

df_constructors2 = df_constructors2.withColumnRenamed("name", "constructor_name").drop("ingestion_date","is_current","start_date","end_date")
display(df_constructors2)

# COMMAND ----------

df_joined = df_results2.join(
    df_races2, "race_id","inner"
).join(
    df_drivers2, "driver_id","inner"
).join(
    df_constructors2, "constructor_id","inner"
)

# COMMAND ----------

display(df_joined)

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import col, avg, count
 
# Load the Silver tables
df_pitstops = spark.read.table("capstone_project_2.silver.pitstop")
df_lap_times = spark.read.table("capstone_project_2.silver.laptimes")
df_qualifying = spark.read.table("capstone_project_2.silver.qualifying")
 
# Aggregate pit stops
df_pitstops_agg = df_pitstops.groupBy("race_id", "driver_id").agg(
    count("stop").alias("total_pitstops"),
    avg("duration").alias("avg_pitstop_duration")
)
 
# Aggregate lap times
df_lap_times_agg = df_lap_times.groupBy("race_id", "driver_id").agg(
    avg("milliseconds").alias("avg_lap_time"),
    count("lap").alias("total_laps")
)
 
# Select relevant columns from qualifying
df_qualifying_agg = df_qualifying.select(
    "race_id",
    "driver_id",
    "constructor_id",
    "position"
)
 
# Join the aggregated data
df_consolidated = df_pitstops_agg.join(
    df_lap_times_agg,
    on=["race_id", "driver_id"],
    how="outer"
).join(
    df_qualifying_agg,
    on=["race_id", "driver_id"],
    how="outer"
)
 
# Write the consolidated DataFrame to a Delta table
df_consolidated.write.format("delta").mode("overwrite").saveAsTable("capstone_project_2.silver.consolidated_race_data")
 
# Display the consolidated DataFrame
display(df_consolidated)

# COMMAND ----------

from pyspark.sql.functions import col, sum, avg, count, when
 
# Load the Silver tables
df_results = spark.read.table("capstone_project_2.silver.results")
df_races = spark.read.table("capstone_project_2.silver.races")
 
# Join results with races to get the season information
df_results_with_season = df_results.join(
    df_races,
    df_results.race_id == df_races.race_id,
    "inner"
).select(
    df_results.driver_id,
    df_races.race_year.alias("season"),
    df_results.points,
    df_results.position
)
 
# Aggregate data for driver performance
df_driver_performance = df_results_with_season.groupBy("driver_id", "season").agg(
    sum("points").alias("total_points"),
    count(when(col("position") == 1, True)).alias("wins"),
    count(when(col("position") <= 3, True)).alias("podiums"),
    avg("position").alias("avg_position")
)
 
df_driver_performance.write.mode("overwrite").saveAsTable("capstone_project_2.gold.driver_performance")
# Display the aggregated DataFrame


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from capstone_project_2.gold.driver_performance

# COMMAND ----------

from pyspark.sql.functions import sum, count, when, col

# Load the Silver tables
df_results = spark.read.table("capstone_project_2.silver.results")
df_races = spark.read.table("capstone_project_2.silver.races")

# Join results with races to get the season information
df_results_with_season = df_results.join(
    df_races,
    df_results.race_id == df_races.race_id,
    "inner"
).select(
    df_results.constructor_id,
    df_races.race_year.alias("season"),
    df_results.points,
    df_results.position
)

# Aggregate data for constructor standings
df_constructor_standings = df_results_with_season.groupBy("constructor_id", "season").agg(
    sum("points").alias("total_points"),
    count(when(col("position") == 1, True)).alias("wins")
)

# Write the aggregated DataFrame to a Delta table
df_constructor_standings.write.mode("overwrite").saveAsTable("capstone_project_2.gold.constructor_standings")
# Display the aggregated DataFrame


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from capstone_project_2.gold.constructor_standings

# COMMAND ----------

from pyspark.sql.functions import col, avg, count
 
# Load the Silver tables
df_pitstops = spark.read.table("capstone_project_2.silver.pitstop")
df_lap_times = spark.read.table("capstone_project_2.silver.laptimes")
df_results = spark.read.table("capstone_project_2.silver.results")
df_races = spark.read.table("capstone_project_2.silver.races")
 
# Join pitstops with races to get the circuit information
df_pitstops_with_circuit = df_pitstops.join(
    df_races,
    df_pitstops.race_id == df_races.race_id,
    "inner"
).select(
    df_races.circuit_id,
    df_pitstops.race_id,
    df_pitstops.driver_id,
    df_pitstops.stop
)
 
# Aggregate pit stop frequencies per circuit
df_pitstops_agg = df_pitstops_with_circuit.groupBy("circuit_id").agg(
    count("stop").alias("total_pitstops")
)
 
# Join lap times with races to get the circuit information
df_lap_times_with_circuit = df_lap_times.join(
    df_races,
    df_lap_times.race_id == df_races.race_id,
    "inner"
).select(
    df_races.circuit_id,
    df_lap_times.race_id,
    df_lap_times.driver_id,
    df_lap_times.milliseconds
)
 
# Aggregate average lap times per circuit
df_lap_times_agg = df_lap_times_with_circuit.groupBy("circuit_id").agg(
    avg("milliseconds").alias("avg_lap_time")
)
 
# Join results with races to get the circuit information
df_results_with_circuit = df_results.join(
    df_races,
    df_results.race_id == df_races.race_id,
    "inner"
).select(
    df_races.circuit_id,
    df_results.race_id,
    df_results.driver_id,
    df_results.position
)
 
# Aggregate finishing positions per circuit
df_results_agg = df_results_with_circuit.groupBy("circuit_id").agg(
    avg("position").alias("avg_position")
)
 
# Join the aggregated data
df_circuit_insights = df_pitstops_agg.join(
    df_lap_times_agg,
    on="circuit_id",
    how="outer"
).join(
    df_results_agg,
    on="circuit_id",
    how="outer"
)
df_circuit_insights = df_circuit_insights.dropna()
df_circuit_insights.write.mode("overwrite").saveAsTable("capstone_project_2.gold.circuit_insights")
 
# Display the aggregated DataFrame



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from capstone_project_2.gold.circuit_insights

# COMMAND ----------


