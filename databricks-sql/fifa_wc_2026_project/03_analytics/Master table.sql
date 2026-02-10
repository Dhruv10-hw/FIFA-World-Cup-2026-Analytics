-- Databricks notebook source
USE workspace.default;

CREATE OR REPLACE TABLE gold_wc2026_team_rankings AS
SELECT
    t.team_name,
    t.group_id,
    t.slot_status,
    t.confederation,
    t.host_flag,

    f.rank AS fifa_rank,
    f.total_points AS fifa_points

FROM silver_wc2026_team_universe t
LEFT JOIN silver_fifa_rankings f
    ON t.team_name = f.team_name_std;

-- COMMAND ----------

SELECT *
FROM gold_wc2026_team_rankings
ORDER BY group_id, team_name
LIMIT 20;

-- COMMAND ----------

USE workspace.default;

CREATE OR REPLACE TABLE gold_wc2026_team_master AS
SELECT
    t.team_name,
    t.group_id,
    t.slot_status,
    t.confederation,
    t.host_flag,

    -- FIFA rankings
    t.fifa_rank,
    t.fifa_points,

    -- Recent form metrics
    rf.matches_played,
    rf.wins,
    rf.draws,
    rf.losses,
    rf.win_percentage,
    rf.goals_scored,
    rf.goals_conceded,
    rf.goals_per_match,
    rf.goals_conceded_per_match,

    -- World Cup performance metrics
    wc.wc_matches_played,
    wc.wc_wins,
    wc.wc_draws,
    wc.wc_losses,
    wc.wc_win_percentage,
    wc.wc_goals_scored,
    wc.wc_goals_conceded,
    wc.wc_goals_per_match,
    wc.wc_goals_conceded_per_match

FROM gold_wc2026_team_rankings t
LEFT JOIN gold_recent_form_metrics rf
    ON t.team_name = rf.team_name
LEFT JOIN gold_wc_performance_metrics wc
    ON t.team_name = wc.team_name;

SELECT COUNT(*) FROM gold_wc2026_team_master;

SELECT
  team_name,
  group_id,
  fifa_rank,
  win_percentage,
  wc_win_percentage
FROM gold_wc2026_team_master
ORDER BY group_id, team_name
LIMIT 20;
