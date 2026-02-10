# Databricks notebook source
from pyspark.sql import functions as F

results = spark.table("bronze_recent_results")
mapping = spark.table("silver_country_name_mapping")

results.printSchema()


# COMMAND ----------

results_home_std = (
    results
    .join(
        mapping,
        results["home_team"] == mapping["raw_team_name"],
        "left"
    )
    .withColumn(
        "home_team_std",
        F.coalesce("standard_team_name", "home_team")
    )
    .drop("standard_team_name", "raw_team_name")
)


# COMMAND ----------

results_full_std = (
    results_home_std
    .join(
        mapping,
        results_home_std["away_team"] == mapping["raw_team_name"],
        "left"
    )
    .withColumn(
        "away_team_std",
        F.coalesce("standard_team_name", "away_team")
    )
    .drop("standard_team_name", "raw_team_name")
)


# COMMAND ----------

display(
    results_full_std
    .select("home_team", "home_team_std", "away_team", "away_team_std")
    .distinct()
    .limit(50)
)


# COMMAND ----------

results_full_std.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_recent_results")
