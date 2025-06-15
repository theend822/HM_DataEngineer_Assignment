{{ config(
    materialized='table',
    partition_by={
        "field": "ds",
        "data_type": "string"
    },
) }}

/*
================================================================================
METRICS TABLE: AGG_USER_RETENTION
================================================================================

This model calculates user retention rates across cohorts. 
Each row represents cohort performance at different
week offsets from their first activity week.

For better visualization, this model only calculates retention up
to 12 weeks after the cohort's first activity week.

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
    MIN({{ date_trunc_sunday_of_the_week('event_time') }}) AS cohort_week
  FROM {{ ref('fct_events') }}
  WHERE ds >= '2025-01-01'
  GROUP BY 1
),

user_weekly_activity AS (
  SELECT 
    user_id, 
    {{ date_trunc_sunday_of_the_week('event_time') }} AS activity_week
  FROM {{ ref('fct_events') }}
  WHERE ds >= '2025-01-01'
  GROUP BY 1, 2
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
  GROUP BY 1, 2
),

cohort_sizes AS (
  SELECT 
    cohort_week,
    retained_users AS cohort_size
  FROM retention_base 
  WHERE week_offset = 0
)

SELECT
  r.cohort_week,
  r.week_offset,
  r.retained_users,
  c.cohort_size,
  ROUND(
  r.retained_users * 100.0 / NULLIF(c.cohort_size, 0), 2
  ) AS retention_rate,
  CURRENT_DATE::VARCHAR AS ds
FROM retention_base r
JOIN cohort_sizes c ON r.cohort_week = c.cohort_week

