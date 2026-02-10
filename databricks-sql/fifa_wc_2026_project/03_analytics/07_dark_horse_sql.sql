-- Databricks notebook source
USE workspace.default;

CREATE OR REPLACE TEMP VIEW power_score_threshold AS
SELECT
    percentile_approx(final_power_score, 0.75) AS top_25_threshold
FROM gold_final_power_score;

SELECT * FROM power_score_threshold;

CREATE OR REPLACE TABLE gold_dark_horse_teams AS
SELECT
    fps.team_name,
    fps.group_id,
    fps.confederation,
    fps.final_power_score,
    r.fifa_rank,

    -- Explain WHY they are dark horses
    CASE
        WHEN r.fifa_rank > 20 THEN 'Underrated by FIFA'
        ELSE '—'
    END AS fifa_reason,

    'High overall performance score' AS performance_reason

FROM gold_final_power_score fps
JOIN gold_wc2026_team_rankings r
    ON fps.team_name = r.team_name
CROSS JOIN power_score_threshold p

WHERE
    r.fifa_rank > 20
    AND fps.final_power_score >= p.top_25_threshold;

SELECT *
FROM gold_dark_horse_teams
ORDER BY final_power_score DESC;

SELECT COUNT(*) FROM gold_dark_horse_teams;