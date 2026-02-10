# Databricks notebook source
fifa = spark.table("bronze_fifa_rankings")
mapping = spark.table("silver_country_name_mapping")

# COMMAND ----------

# DBTITLE 1,Standardize FIFA team names (minimal fix)
from pyspark.sql import functions as F
fifa = spark.table("bronze_fifa_rankings")
mapping = spark.table("silver_country_name_mapping")

fifa_clean = (
    fifa
    .join(
        mapping,
        fifa["team"] == mapping["raw_team_name"],
        "left"
    )
    .withColumn(
        "team_name_std",
        F.coalesce("standard_team_name", "team")
    )
    .drop("standard_team_name","raw_team_name")
)

display(fifa_clean.select("team","team_name_std").distinct())


# COMMAND ----------

# DBTITLE 1,Display distinct standardized team names
fifa_clean.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_fifa_rankings")