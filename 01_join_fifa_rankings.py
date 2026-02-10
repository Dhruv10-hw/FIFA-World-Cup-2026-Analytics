# Databricks notebook source
###rough work
# Load the existing teams table
old_teams = spark.table("workspace.default.silver_wc2026_teams")
old_teams.printSchema()
final_teams = (
    old_teams
    .withColumnRenamed("team", "team_name")
    .withColumnRenamed("group", "group_id")
    .withColumnRenamed("status", "slot_status")
)

final_teams.printSchema()
final_teams.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_wc2026_team_universe")
display(spark.table("silver_wc2026_team_universe").limit(5))


# COMMAND ----------

from pyspark.sql import functions as F

wc_teams = spark.table("silver_wc2026_team_universe")
fifa = spark.table("silver_fifa_rankings")

fifa_rank_clean = fifa.select(
    "team_name_std",
    F.col("rank").alias("fifa_rank"),
    F.col("total_points").alias("fifa_points")
)

wc_with_fifa = (
    wc_teams
    .join(
        fifa_rank_clean,
        wc_teams["team_name"] == fifa_rank_clean["team_name_std"],
        "left"
    )
    .drop("team_name_std")
)

display(
    wc_with_fifa
    .select(
        "team_name",
        "group_id",
        "slot_status",
        "fifa_rank",
        "fifa_points"
    )
    .orderBy("group_id", "team_name")
)


# COMMAND ----------

