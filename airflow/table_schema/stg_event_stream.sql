DROP TABLE IF EXISTS stg_event_stream;

-- defaulting every column to VARCHAR
CREATE TABLE stg_event_stream (
    event_time VARCHAR,
    user_id VARCHAR,
    event_type VARCHAR,
    transaction_category VARCHAR,
    miles_amount VARCHAR,
    platform VARCHAR,
    utm_source VARCHAR,
    country VARCHAR
);