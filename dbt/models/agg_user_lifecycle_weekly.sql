{{ config(
    materialized='table',
    partition_by={
        "field": "ds",
        "data_type": "string"
    },
) }}

/*
================================================================================
METRICS TABLE: USER LIFECYCLE COUNTS BY WEEK
================================================================================

This model calculates weekly user lifecycle status counts using growth accounting 
methodology to track user behavior patterns and business health.

GRANULARITY: week_start_date + acq_source + country + user_status + ds (partition key)

METRICS CALCULATED:
• User lifecycle statuses: new, retained, resurrected, dormant, churned
• user_count: Number of users in each lifecycle status per week

DATA SOURCES:
• fct_events
• dim_users

================================================================================
USER LIFECYCLE DEFINITIONS (4-week lookback window)
================================================================================

(Assuming "Week 23 2025-06-02" is selected)

* NEW: First time ever active in Week 23

* RETAINED: Active in Week 23 AND was active within the previous 4 weeks (Week 19-22)
    - Example: User active in Week 23, also active in Week 20
    - Example: User active in Week 23, also active in Week 19 and 21

* RESURRECTED: Active in Week 23 BUT last activity was more than 4 weeks ago (before Week 19)
    - Example: User active in Week 23, last seen in Week 16

* DORMANT: NOT active in Week 23 but was active within previous 4 weeks (Week 19-22)
    - Example: User not active in Week 23, but was active in Week 20

* CHURNED: NOT active in Week 23 AND last activity was more than 4 weeks ago (before Week 19)
    - Example: User not active in Week 23, last seen in Week 16

================================================================================
CRITICAL CALCULATION IN STEP 4
================================================================================

This calculates the most recent week (before current week) where user was active.
    
SYNTAX BREAKDOWN:
• MAX(week_start) FILTER (WHERE is_active_this_week = 1) 
    → Only consider weeks where user was active (is_active_this_week = 1)
    → Take the maximum (most recent) week_start date from those weeks

• OVER (PARTITION BY user_id ORDER BY week_start 
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
    → PARTITION BY user_id: Calculate separately for each user
    → ORDER BY week_start: Process weeks chronologically
    → ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING: 
        Look at all rows from the beginning up to (but not including) current row

EXAMPLE for user_id = 'u_1234':
week_start | is_active | last_active_week_before | Explanation
2025-01-05 |     1     |        NULL            | First week, no previous activity
2025-01-12 |     1     |     2025-01-05         | Previous active week was 2025-01-05
2025-01-19 |     0     |     2025-01-12         | Last active was 2025-01-12 
2025-01-26 |     0     |     2025-01-12         | Still 2025-01-12 (no activity since)
2025-02-02 |     1     |     2025-01-12         | Coming back, last active was 2025-01-12
2025-02-09 |     1     |     2025-02-02         | Previous active week was 2025-02-02

This helps determine:
• CHURNED: last_active_week_before < (current_week - 4 weeks)
• DORMANT: last_active_week_before >= (current_week - 4 weeks) AND not active this week
================================================================================
*/


-- Get latest user attributes
WITH latest_users AS (
  SELECT 
    user_id,
    acq_source,
    user_country as country
  FROM {{ ref('dim_users') }}
  WHERE ds = {{ latest_ds('dim_users') }}
),

-- STEP 1: Normalize activity by week
user_weekly_activity AS (
  SELECT
    e.user_id,
    u.acq_source,
    u.country,
    {{ date_trunc_sunday_of_the_week('event_time') }} AS week_start -- start on Sunday
  FROM {{ ref('fct_events') }} e
  JOIN latest_users u ON e.user_id = u.user_id
  WHERE e.ds >= '2025-01-01'
  GROUP BY 1, 2, 3, 4
),

-- STEP 2: Get all weeks and all users
all_weeks AS (
  SELECT DISTINCT week_start FROM user_weekly_activity
),

all_users AS (
  SELECT DISTINCT user_id, acq_source, country FROM user_weekly_activity
),

-- STEP 3: Generate user-week grid (activity flag only)
user_week_grid AS (
  SELECT
    aw.week_start,
    au.user_id,
    au.acq_source,
    au.country,
    CASE WHEN uwa.user_id IS NOT NULL THEN 1 ELSE 0 END AS is_active_this_week
  FROM all_weeks aw
  CROSS JOIN all_users au
  LEFT JOIN user_weekly_activity uwa
    ON aw.week_start = uwa.week_start AND au.user_id = uwa.user_id
),

-- STEP 4: Enrich with history: first week active, past activity flags, and last active week
user_week_with_history AS (
  SELECT
    *,
    MIN(CASE WHEN is_active_this_week = 1 THEN week_start END)
      OVER (PARTITION BY user_id) AS first_ever_week,

    LAG(is_active_this_week, 1) OVER (PARTITION BY user_id ORDER BY week_start) AS active_1_week_ago,
    LAG(is_active_this_week, 2) OVER (PARTITION BY user_id ORDER BY week_start) AS active_2_weeks_ago,
    LAG(is_active_this_week, 3) OVER (PARTITION BY user_id ORDER BY week_start) AS active_3_weeks_ago,
    LAG(is_active_this_week, 4) OVER (PARTITION BY user_id ORDER BY week_start) AS active_4_weeks_ago,

    MAX(week_start) FILTER (WHERE is_active_this_week = 1)
      OVER (PARTITION BY user_id ORDER BY week_start ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
      AS last_active_week_before -- Detailed documentation available in the header above

  FROM user_week_grid
),

-- STEP 5: Classify user status
user_status_calculated AS (
  SELECT
    week_start,
    user_id,
    acq_source,
    country,
    is_active_this_week,

    CASE
      WHEN is_active_this_week = 1 AND week_start = first_ever_week
        THEN 'new'

      WHEN is_active_this_week = 1 AND (
        COALESCE(active_1_week_ago, 0) = 1 OR
        COALESCE(active_2_weeks_ago, 0) = 1 OR
        COALESCE(active_3_weeks_ago, 0) = 1 OR
        COALESCE(active_4_weeks_ago, 0) = 1)
        THEN 'retained'

      WHEN is_active_this_week = 1 AND
        COALESCE(active_1_week_ago, 0) = 0 AND
        COALESCE(active_2_weeks_ago, 0) = 0 AND
        COALESCE(active_3_weeks_ago, 0) = 0 AND
        COALESCE(active_4_weeks_ago, 0) = 0 AND
        week_start > first_ever_week
        THEN 'resurrected'

      WHEN is_active_this_week = 0 AND (
        COALESCE(active_1_week_ago, 0) = 1 OR
        COALESCE(active_2_weeks_ago, 0) = 1 OR
        COALESCE(active_3_weeks_ago, 0) = 1 OR
        COALESCE(active_4_weeks_ago, 0) = 1)
        THEN 'dormant'

      WHEN is_active_this_week = 0 AND
        last_active_week_before < (week_start - INTERVAL '4 weeks') AND
        last_active_week_before IS NOT NULL
        THEN 'churned'

      ELSE NULL
    END AS user_status
  FROM user_week_with_history
  WHERE week_start >= first_ever_week
)

-- STEP 6: Aggregate metrics by week/acq/country/status
SELECT
  'WK' || LPAD(EXTRACT(WEEK FROM week_start)::TEXT, 2, '0') || ' ' || week_start AS cutoff_week,
  acq_source,
  country,
  user_status,
  COUNT(user_id) AS user_count,
  CURRENT_DATE::VARCHAR AS ds
FROM user_status_calculated
WHERE user_status IS NOT NULL
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2, 3, 4