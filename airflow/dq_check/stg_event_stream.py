# Acceptable values for each column in the stg_event_stream table
event_type = [
    "miles_earned",
    "share",
    "miles_redeemed",
    "reward_search",
    "like",
]

transaction_category = [
    "dining",
    "ecommerce",
    "flight",
    "shopping",
    "shopee",
    "hotel",
    "lululemon",
    "apple",
]

platform = [
    "ios",
    "android",
    "web",
]

utm_source = [
    "google",
    "facebook",
    "tiktok",
    "organic",
    "referral",
]

country = [
    "TH",
    "SG",
    "MY",
    "PH",
    "ID",
]


# define SQL statement for data quality checks
# As there is no native support in postgres for data quality checks, a workaround is created where if any issue is found,
# count(*) will be forced to be 9 and then the division calculation will fail
table_name = "stg_event_stream"
sql_template = f"SELECT 1 / CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS dq_check FROM {table_name}"


DQ_CHECKS = {

    "ACCEPT_VALUE_CHECK":{
        "event_type": f"{sql_template} WHERE event_type NOT IN {tuple(event_type)}",
        "transaction_category": f"{sql_template} WHERE event_type not in ('share','like') and transaction_category NOT IN {tuple(transaction_category)}",
        "platform": f"{sql_template} WHERE platform NOT IN {tuple(platform)}",
        "utm_source": f"{sql_template} WHERE utm_source NOT IN {tuple(utm_source)}",
        "country": f"{sql_template} WHERE country NOT IN {tuple(country)}",
    },

    "NULL_CHECK": {
        "event_time": f"{sql_template} WHERE event_time IS NULL",
        "user_id": f"{sql_template} WHERE user_id IS NULL",
        "event_type": f"{sql_template} WHERE event_type IS NULL",
        "transaction_category": f"{sql_template} WHERE event_type not in ('share','like') and transaction_category IS NULL",
        "miles_amount": f"{sql_template} WHERE event_type not in ('share','like','reward_search') and miles_amount IS NULL",
        "platform": f"{sql_template} WHERE platform IS NULL",
        "utm_source": f"{sql_template} WHERE utm_source IS NULL",
        "country": f"{sql_template} WHERE country IS NULL",
    },

    "FORMAT_CHECK": {
        "event_time": f"{sql_template} WHERE event_time !~ '^\\d{{4}}-\\d{{2}}-\\d{{2}} \\d{{2}}:\\d{{2}}:\\d{{2}}\\.\\d{{6}}$'",
        "user_id": f"{sql_template} WHERE user_id !~ '^u_\\d{{4}}$'",
    },
   
}

# TO-DO: create function to automatically generate DQ_CHECKS dict based on config dict