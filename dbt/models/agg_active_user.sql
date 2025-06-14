{{ config(
    materialized='table',
    partition_by={
        "field": "ds",
        "data_type": "string"
    },
) }}


/*
================================================================================
METRICS TABLE: ACTIVE USERS
================================================================================

This model calculates active user counts across multiple time granularities
for dashboard and analytics consumption.

GRANULARITY: period_start_date + period_type + acq_source + country + ds (partition key)

METRICS CALCULATED:
• Daily Active Users (DAU): Unique users active on each calendar day
• Weekly Active Users (WAU): Unique users active in each Sunday-Saturday week  
• Monthly Active Users (MAU): Unique users active in each calendar month

DATA SOURCES:
• fct_events
• dim_users

================================================================================
*/


WITH base_events AS (
  SELECT 
    e.user_id,
    e.event_time,
    u.acq_source,
    u.user_country as country
  FROM {{ ref('fct_events') }} e
  JOIN {{ ref('dim_users') }} u 
  ON u.ds = {{ latest_ds('dim_users') }} AND e.user_id = u.user_id
  WHERE e.ds >= '2025-01-01'
),

daily_active AS (
  SELECT 
    DATE(event_time) as period_start_date,
    'daily' as period_type,
    acq_source,
    country,
    COUNT(DISTINCT user_id) as active_user_count
  FROM base_events
  GROUP BY DATE(event_time), acq_source, country
),

weekly_active AS (
  SELECT 
    DATE_TRUNC('week', event_time + INTERVAL '1 day')::DATE - 1 as period_start_date, -- start on Sunday
    'weekly' as period_type,
    acq_source,
    country,
    COUNT(DISTINCT user_id) as active_user_count
  FROM base_events
  GROUP BY DATE_TRUNC('week', event_time), acq_source, country
),

monthly_active AS (
  SELECT 
    DATE_TRUNC('month', event_time)::DATE as period_start_date,
    'monthly' as period_type,
    acq_source,
    country,
    COUNT(DISTINCT user_id) as active_user_count
  FROM base_events
  GROUP BY DATE_TRUNC('month', event_time), acq_source, country
)

SELECT 
  period_start_date,
  period_type,
  acq_source,
  country,
  active_user_count,
  CURRENT_DATE::VARCHAR AS ds
FROM daily_active

UNION ALL

SELECT 
  period_start_date,
  period_type,
  acq_source,
  country,
  active_user_count,
  CURRENT_DATE::VARCHAR AS ds
FROM weekly_active  

UNION ALL

SELECT 
  period_start_date,
  period_type,
  acq_source,
  country,
  active_user_count,
  CURRENT_DATE::VARCHAR AS ds
FROM monthly_active

ORDER BY period_start_date, period_type, acq_source, country