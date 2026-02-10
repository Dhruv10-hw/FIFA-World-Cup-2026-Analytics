# Databricks notebook source
fifa_rank = spark.table("fifa_2026_ranking")
display(fifa_rank)

# COMMAND ----------

wc_matches = spark.table("fifa_world_cup_1930_2022_all_match_dataset")
display(wc_matches)

# COMMAND ----------

recent_results = spark.table("results")
display(recent_results)

# COMMAND ----------

# DBTITLE 1,Write DataFrames as Delta tables (sanitize wc_matches columns)
import re

def sanitize_column(col):
    # Replace invalid characters with underscores
    return re.sub(r'[ ,;{}()\n\t=]', '_', col)

wc_matches_clean = wc_matches.toDF(*[sanitize_column(c) for c in wc_matches.columns])

fifa_rank.write.format("delta").mode("overwrite").saveAsTable("bronze_fifa_rankings")
wc_matches_clean.write.format("delta").mode("overwrite").saveAsTable("bronze_wc_matches")
recent_results.write.format("delta").mode("overwrite").saveAsTable("bronze_recent_results")
