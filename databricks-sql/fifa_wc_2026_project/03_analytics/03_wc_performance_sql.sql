-- Databricks notebook source
CREATE OR REPLACE TEMP VIEW wc_team_matches AS

-- Home team view
SELECT
    home_team_std AS team_name,
    home_team_score AS goals_for,
    away_team_score AS goals_against,
    CASE
        WHEN home_team_score > away_team_score THEN 1 ELSE 0
    END AS win,
    CASE
        WHEN home_team_score = away_team_score THEN 1 ELSE 0
    END AS draw,
    CASE
        WHEN home_team_score < away_team_score THEN 1 ELSE 0
    END AS loss
FROM silver_wc_matches

UNION ALL

-- Away team view
SELECT
    away_team_std AS team_name,
    away_team_score AS goals_for,
    home_team_score AS goals_against,
    CASE
        WHEN away_team_score > home_team_score THEN 1 ELSE 0
    END AS win,
    CASE
        WHEN away_team_score = home_team_score THEN 1 ELSE 0
    END AS draw,
    CASE
        WHEN away_team_score < home_team_score THEN 1 ELSE 0
    END AS loss
FROM silver_wc_matches;


-- COMMAND ----------

SELECT *
FROM wc_team_matches
LIMIT 20;


-- COMMAND ----------

CREATE OR REPLACE TABLE gold_wc_performance_metrics AS
SELECT
    team_name,
    COUNT(*) AS wc_matches_played,
    SUM(win) AS wc_wins,
    SUM(draw) AS wc_draws,
    SUM(loss) AS wc_losses,
    SUM(goals_for) AS wc_goals_scored,
    SUM(goals_against) AS wc_goals_conceded,
    ROUND(SUM(win) * 1.0 / COUNT(*), 3) AS wc_win_percentage,
    ROUND(SUM(goals_for) * 1.0 / COUNT(*), 2) AS wc_goals_per_match,
    ROUND(SUM(goals_against) * 1.0 / COUNT(*), 2) AS wc_goals_conceded_per_match
FROM wc_team_matches
GROUP BY team_name;


-- COMMAND ----------

SELECT *
FROM gold_wc_performance_metrics
ORDER BY wc_matches_played DESC
LIMIT 20;
