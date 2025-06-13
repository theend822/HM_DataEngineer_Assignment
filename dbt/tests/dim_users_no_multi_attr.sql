-- Test: dim_users should not have users with multiple acq sources or countries
-- Fails if: Any user appears with different utm_source or country values

SELECT 
    * 
FROM (
    SELECT 
    user_id,
    COUNT(DISTINCT utm_source) as acq_source_count,
    COUNT(DISTINCT country) as country_count
    FROM {{ ref('fct_event_stream') }}
    GROUP BY 1
)
WHERE acq_source_count > 1 OR country_count > 1
