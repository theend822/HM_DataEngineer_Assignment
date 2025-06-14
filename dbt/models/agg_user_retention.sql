{{ config(
    materialized='table',
    partition_by={
        "field": "ds",
        "data_type": "string"
    },
) }}

/*
================================================================================
METRICS TABLE: USER RETENTION
================================================================================

This model calculates user retention rates across cohorts using your original
triangle retention logic. Each row represents cohort performance at different
week offsets from their first activity week.

GRANULARITY: cohort_week + week_offset + ds (partition key)

METRICS CALCULATED:
• retained_users: Count of users from cohort active in week N
• cohort_size: Total users in the cohort (week 0)  
• retention_rate: Percentage of cohort retained in week N

DATA SOURCES:
• fct_events

================================================================================
*/

WITH user_cohorts AS (
  SELECT 
    user_id, 
    DATE_TRUNC('week', MIN(event_time))::DATE AS cohort_week
  FROM {{ ref('fct_events') }}
  WHERE ds >= '2025-01-01'
  GROUP BY user_id
),

user_weekly_activity AS (
  SELECT 
    user_id, 
    DATE_TRUNC('week', event_time)::DATE AS activity_week
  FROM {{ ref('fct_events') }}
  WHERE ds >= '2025-01-01'
  GROUP BY user_id, DATE_TRUNC('week', event_time)
),

cohort_activity AS (
  SELECT
    a.user_id,
    c.cohort_week,
    a.activity_week,
    DATE_PART('week', a.activity_week - c.cohort_week) AS week_offset
  FROM user_cohorts c
  JOIN user_weekly_activity a ON c.user_id = a.user_id
),

retention_base AS (
  SELECT
    cohort_week,
    week_offset,
    COUNT(DISTINCT user_id) AS retained_users
  FROM cohort_activity
  WHERE week_offset >= 0 AND week_offset <= 12
  GROUP BY cohort_week, week_offset
),

retention_with_metrics AS (
  SELECT
    cohort_week,
    week_offset,
    retained_users,
    -- Calculate cohort size (week 0 retained users)
    FIRST_VALUE(retained_users) OVER (
      PARTITION BY cohort_week 
      ORDER BY week_offset
    ) AS cohort_size
  FROM retention_base
)

SELECT
  cohort_week,
  week_offset,
  retained_users,
  cohort_size,
  ROUND(
    retained_users * 100.0 / NULLIF(cohort_size, 0), 2
  ) AS retention_rate,
  CURRENT_DATE::VARCHAR AS ds
FROM retention_with_metrics
ORDER BY cohort_week, week_offset
