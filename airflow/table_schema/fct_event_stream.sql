DROP TABLE IF EXISTS fct_event_stream;

CREATE TABLE fct_event_stream (
    event_time TIMESTAMP,
    user_id VARCHAR(50),
    event_type VARCHAR(255),
    transaction_category VARCHAR(50),
    miles_amount DECIMAL(10,2),
    platform VARCHAR(50),
    utm_source VARCHAR(50),
    country VARCHAR(10)
);