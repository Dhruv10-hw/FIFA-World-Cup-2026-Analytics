-- Databricks notebook source
USE workspace.default;

CREATE OR REPLACE TEMP VIEW team_power_components AS
SELECT
    team_name,
    group_id,
    slot_status,
    confederation,
    host_flag,

    -- FIFA strength (lower rank = stronger)
    CASE 
        WHEN fifa_rank IS NOT NULL THEN 1.0 / fifa_rank
        ELSE NULL
    END AS fifa_strength,

    -- Recent form strength
    win_percentage AS recent_form_strength,

    -- World Cup experience strength
    wc_win_percentage AS wc_strength

FROM gold_wc2026_team_master;

SELECT *
FROM team_power_components
ORDER BY group_id, team_name
LIMIT 20;

CREATE OR REPLACE TABLE gold_final_power_score AS
SELECT
    team_name,
    group_id,
    slot_status,
    confederation,
    host_flag,

    fifa_strength,
    recent_form_strength,
    wc_strength,

    ROUND(
        (
            COALESCE(fifa_strength, 0) +
            COALESCE(recent_form_strength, 0) +
            COALESCE(wc_strength, 0)
        ) / 3
    , 4) AS final_power_score

FROM team_power_components;

SELECT
    team_name,
    group_id,
    final_power_score,
    RANK() OVER (ORDER BY final_power_score DESC) AS global_rank
FROM gold_final_power_score
ORDER BY global_rank;

SELECT COUNT(*) FROM gold_final_power_score;

SELECT team_name, final_power_score
FROM gold_final_power_score
ORDER BY final_power_score DESC
LIMIT 10;
