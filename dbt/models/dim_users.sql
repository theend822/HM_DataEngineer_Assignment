{{ config(
    materialized='table',
    partition_by={
        "field": "ds",
        "data_type": "string"
    },
    pre_hook="{{ ref('dbt/tests/dim_users_no_multi_attr') }}" -- if dq check of upstream fails, pipeline won't run
) }}

WITH user_aggregates AS (
    SELECT
        user_id,
        -- User attributes
        utm_source AS acq_source,
        country AS user_country,
        platform,
        
        -- Miles summary
        SUM(CASE WHEN event_type = 'miles_earned' THEN miles_amount ELSE 0 END) AS total_miles_earned,
        SUM(CASE WHEN event_type = 'miles_redeemed' THEN miles_amount ELSE 0 END) AS total_miles_redeemed,
        
        -- Activity flags
        MAX(event_time) AS latest_event_time,
        MAX(CASE WHEN event_time >= CURRENT_DATE - INTERVAL '7 days' THEN 1 ELSE 0 END) AS active_in_7days,
        MAX(CASE WHEN event_time >= CURRENT_DATE - INTERVAL '30 days' THEN 1 ELSE 0 END) AS active_in_30days,
        MAX(CASE WHEN event_time >= CURRENT_DATE - INTERVAL '90 days' THEN 1 ELSE 0 END) AS active_in_90days
        
    FROM {{ ref('fct_event_stream') }}
    GROUP BY 1,2,3,4
),

latest_events AS (
    SELECT 
        user_id,
        event_time,
        event_type,
        transaction_category,
        miles_amount,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_time DESC) as rn
    FROM {{ ref('fct_event_stream') }}
)

SELECT
    user_id,
    acq_source,
    user_country,
    platform,
    total_miles_earned,
    total_miles_redeemed,
    (total_miles_earned - total_miles_redeemed) AS miles_balance,
    CASE WHEN active_in_7days = 1 THEN TRUE ELSE FALSE END AS active_in_7days,
    CASE WHEN active_in_30days = 1 THEN TRUE ELSE FALSE END AS active_in_30days,
    CASE WHEN active_in_90days = 1 THEN TRUE ELSE FALSE END AS active_in_90days,
    
    -- Latest event as a JSON map
    JSON_BUILD_OBJECT(
        'event_time', le.event_time,
        'event_type', le.event_type,
        'transaction_category', le.transaction_category,
        'miles_amount', le.miles_amount
    ) AS latest_event_json,
    
    -- Partition column (snapshot date)
    CURRENT_DATE::VARCHAR AS ds
    
FROM user_aggregates ua
LEFT JOIN latest_events le 
ON ua.user_id = le.user_id 
AND le.rn = 1