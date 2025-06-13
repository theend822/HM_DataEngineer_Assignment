-- Test: fct_events transaction_category should only exist for applicable events  
-- Fails if: transaction_category exists where it shouldn't, or missing where it should exist
SELECT *
FROM {{ ref('fct_events') }}
WHERE (event_type IN ('share', 'like') AND transaction_category IS NOT NULL)
   OR (event_type IN ('miles_earned', 'miles_redeemed', 'reward_search') AND transaction_category IS NULL)