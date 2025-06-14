{{ config(
    materialized='table',
    unique_key='event_id',
    partition_by={
        "field": "ds",
        "data_type": "string"
    }
) }}

/*
================================================================================
FACT TABLE: FCT_EVENTS
================================================================================

This fact table stores all user events with enhanced attributes for 
analytics and metrics calculation.

GRANULARITY: event_id (made of 'event_time', 'user_id', 'event_type')

EVENT TYPES:
• miles_earned: User earned miles from transactions
• miles_redeemed: User spent miles on rewards  
• share: User shared content (social engagement)
• like: User liked content (social engagement)
• reward_search: User searched for rewards

KEY FEATURES:
• JSON storage: Flexible event_json field for additional attributes

DATA SOURCES:
• fct_event_stream

================================================================================
*/

SELECT
    -- Create surrogate key for events
    {{ dbt_utils.generate_surrogate_key(['event_time', 'user_id', 'event_type']) }} AS event_id,
    
    -- Core event attributes
    event_time,

    -- Date parts as integers
    EXTRACT(YEAR FROM event_time) AS event_year,
    EXTRACT(MONTH FROM event_time) AS event_month, 
    EXTRACT(DAY FROM event_time) AS event_day,
    EXTRACT(WEEK FROM event_time)::INT AS event_week,
    
    user_id,
    event_type,
    
    -- Event data as a JSON map
    JSON_BUILD_OBJECT(
        'event_type', event_type,
        'transaction_category', transaction_category,
        'miles_amount', miles_amount
    ) AS event_json,
    
    platform,
    DATE(event_time)::VARCHAR AS ds  -- YYYY-MM-DD string format
    
FROM {{ source('hm_datamart', 'fct_event_stream') }}