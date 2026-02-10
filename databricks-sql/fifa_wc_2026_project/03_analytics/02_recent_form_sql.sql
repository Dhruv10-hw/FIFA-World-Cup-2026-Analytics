-- Databricks notebook source
-- DBTITLE 1,Create recent_team_matches temp view (fix column name)
CREATE OR REPLACE TEMP VIEW recent_team_matches AS

-- Home team perspective
SELECT
    home_team_std AS team_name,
    date,
    home_score AS goals_for,
    away_score AS goals_against,
    CASE
        WHEN home_score > away_score THEN 1
        ELSE 0
    END AS win,
    CASE
        WHEN home_score = away_score THEN 1
        ELSE 0
    END AS draw,
    CASE
        WHEN home_score < away_score THEN 1
        ELSE 0
    END AS loss
FROM silver_recent_results

UNION ALL

-- Away team perspective
SELECT
    away_team_std AS team_name,
    date,
    away_score AS goals_for,
    home_score AS goals_against,
    CASE
        WHEN away_score > home_score THEN 1
        ELSE 0
    END AS win,
    CASE
        WHEN away_score = home_score THEN 1
        ELSE 0
    END AS draw,
    CASE
        WHEN away_score < home_score THEN 1
        ELSE 0
    END AS loss
FROM silver_recent_results;

-- COMMAND ----------

SELECT *
FROM recent_team_matches
LIMIT 20;

-- COMMAND ----------

CREATE OR REPLACE TABLE gold_recent_form_metrics AS
SELECT
    team_name,
    COUNT(*) AS matches_played,
    SUM(win) AS wins,
    SUM(draw) AS draws,
    SUM(loss) AS losses,
    SUM(goals_for) AS goals_scored,
    SUM(goals_against) AS goals_conceded,
    ROUND(SUM(win) * 1.0 / COUNT(*), 3) AS win_percentage,
    ROUND(SUM(goals_for) * 1.0 / COUNT(*), 2) AS goals_per_match,
    ROUND(SUM(goals_against) * 1.0 / COUNT(*), 2) AS goals_conceded_per_match
FROM recent_team_matches
GROUP BY team_name;


-- COMMAND ----------

SELECT *
FROM gold_recent_form_metrics
ORDER BY win_percentage DESC
LIMIT 20;

CREATE OR REPLACE TABLE gold_recent_form_metrics AS
SELECT
    team_name,
    COUNT(*) AS matches_played,
    SUM(win) AS wins,
    SUM(draw) AS draws,
    SUM(loss) AS losses,
    SUM(goals_for) AS goals_scored,
    SUM(goals_against) AS goals_conceded,
    ROUND(SUM(win) * 1.0 / COUNT(*), 3) AS win_percentage,
    ROUND(SUM(goals_for) * 1.0 / COUNT(*), 2) AS goals_per_match,
    ROUND(SUM(goals_against) * 1.0 / COUNT(*), 2) AS goals_conceded_per_match
FROM recent_team_matches
GROUP BY team_name;

SELECT *
FROM gold_recent_form_metrics
ORDER BY win_percentage DESC
LIMIT 20;
