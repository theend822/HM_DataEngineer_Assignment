-- Test: fct_events miles_amount should only exist for transactional events
-- Fails if: miles_amount exists where it shouldn't, or missing where it should exist
SELECT *
FROM {{ ref('fct_events') }}
WHERE (event_type IN ('share', 'like') AND (event_json ->> 'miles_amount') IS NOT NULL)
   OR (event_type = 'reward_search' AND (event_json ->> 'miles_amount') IS NOT NULL)
   OR (event_type IN ('miles_earned', 'miles_redeemed') AND (event_json ->> 'miles_amount') IS NULL)