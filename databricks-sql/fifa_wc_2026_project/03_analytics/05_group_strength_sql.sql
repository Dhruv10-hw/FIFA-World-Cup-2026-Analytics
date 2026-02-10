-- Databricks notebook source
USE workspace.default;

CREATE OR REPLACE TEMP VIEW team_strength_components AS
SELECT
    team_name,
    group_id,

    -- FIFA strength (inverse rank)
    CASE 
        WHEN fifa_rank IS NOT NULL THEN 1.0 / fifa_rank
        ELSE NULL
    END AS fifa_strength,

    -- Recent form
    win_percentage AS recent_form_strength,

    -- World Cup experience
    wc_win_percentage AS wc_strength

FROM gold_wc2026_team_master;

SELECT *
FROM team_strength_components
ORDER BY group_id, team_name
LIMIT 20;

CREATE OR REPLACE TABLE gold_group_strength_index AS
SELECT
    group_id,

    COUNT(team_name) AS teams_in_group,

    AVG(fifa_strength)        AS avg_fifa_strength,
    AVG(recent_form_strength) AS avg_recent_form,
    AVG(wc_strength)          AS avg_wc_strength,

    -- Final Group Strength Index (simple average)
    ROUND(
        (
            AVG(fifa_strength) +
            AVG(recent_form_strength) +
            AVG(wc_strength)
        ) / 3
    , 4) AS group_strength_index

FROM team_strength_components
GROUP BY group_id;

SELECT *
FROM gold_group_strength_index
ORDER BY group_strength_index DESC;

SELECT COUNT(*) FROM gold_group_strength_index;