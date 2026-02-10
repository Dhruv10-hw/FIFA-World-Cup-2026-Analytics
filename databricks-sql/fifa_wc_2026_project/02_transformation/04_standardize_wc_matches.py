# Databricks notebook source
from pyspark.sql import functions as F

wc = spark.table("bronze_wc_matches")
mapping = spark.table("silver_country_name_mapping")

# COMMAND ----------

wc.printSchema()

# COMMAND ----------

wc = spark.table("bronze_wc_matches")
mapping = spark.table("silver_country_name_mapping")

wc_home_std = (
    wc
    .join(
        mapping,
        wc["Home_Team_Name"] == mapping["raw_team_name"],
        "left"
    )
    .withColumn(
        "home_team_std",
        F.coalesce("standard_team_name", "Home_Team_Name")
    )
    .drop("standard_team_name", "raw_team_name")
)

# COMMAND ----------

wc_full_std = (
    wc_home_std
    .join(
        mapping,
        wc_home_std["Away_Team_Name"] == mapping["raw_team_name"],
        "left"
    )
    .withColumn(
        "away_team_std",
        F.coalesce("standard_team_name", "Away_Team_Name")
    )
    .drop("standard_team_name", "raw_team_name")
)


# COMMAND ----------

display(
    wc_full_std
    .select("Home_Team_Name", "home_team_std", "Away_Team_Name", "away_team_std")
    .distinct()
    .limit(50)
)


# COMMAND ----------

wc_full_std.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_wc_matches")
