# Databricks notebook source
mapping_data = [

# --- Czech Republic ---
("Czech Republic","Czechia"),
("Czech Republic","Czech Rep."),
("Czech Republic","CZE"),

# --- DR Congo ---
("DR Congo","Congo DR"),
("DR Congo","DR Congo"),
("DR Congo","Democratic Republic of the Congo"),

# --- Korea Republic ---
("Korea Republic","South Korea"),
("Korea Republic","Korea Rep."),
("Korea Republic","Republic of Korea"),

# --- Iran ---
("IR Iran","Iran"),

# --- Ivory Coast ---
("Côte d'Ivoire","Ivory Coast"),

# --- USA ---
("United States","USA"),
("United States","U.S.A."),
("United States","United States of America"),

# --- Cape Verde ---
("Cabo Verde","Cape Verde"),

# --- North Macedonia ---
("North Macedonia","Macedonia"),
("North Macedonia","FYR Macedonia"),

# --- Bolivia ---
("Bolivia","Bolivia (Plurinational State of)"),

# --- Bosnia ---
("Bosnia and Herzegovina","Bosnia & Herzegovina"),

# --- Curacao ---
("Curaçao","Curacao")
]

columns = ["standard_team_name","raw_team_name"]

country_mapping_df = spark.createDataFrame(mapping_data, columns)
display(country_mapping_df)

country_mapping_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_country_name_mapping")