# HeyMax Loyalty Program Analytics Platform

A comprehensive data engineering solution for loyalty program analytics, featuring end-to-end data pipeline orchestration, robust data quality checks, and actionable business metrics.

## Business Metrics & Analytics

This platform powers three core business intelligence areas:

### **User Retention Analysis**
- Cohort-based retention tracking up to 12 weeks
- Week-over-week retention rates and retention triangle visualization

### **User Lifecycle Management** 
- Growth accounting with 4-week lookback window
- User status: New, Retained, Resurrected, Dormant, Churned
- Segmentation by acquisition source and geography

### **Active User Metrics**
- Multi-granularity tracking: Daily, Weekly, Monthly Active Users
- Cross-dimensional analysis by acquisition channel and country

## Architecture & Data Flow


### Key Design Principles

**Modularity**: Each component (ingestion, transformation, metrics) is independently deployable and testable
**Data Quality First**: Comprehensive validation at every stage - from raw ingestion to final metrics
**Consistent Granularity**: All models use Sunday-starting weeks for consistent time-based analysis
**Scalability**: Partitioned tables and efficient joins designed for growing data volumes

## Quick Start

### 1. Environment Setup
Configure your database credentials in .env
- POSTGRES_USER=your_user
- POSTGRES_PASSWORD=your_password
- POSTGRES_DB=heymax_loyalty
- POSTGRES_HOST=postgres
- POSTGRES_PORT=5432

### 2. Raw data
Save raw data in *airflow/raw_data* folder

### 3. Access Applications
- Airflow UI: http://localhost:8080 (admin/admin)
- Metabase: http://localhost:3000
- PostgreSQL: localhost:5432

### 4. Run Data Pipeline
1. Navigate to Airflow UI
2. Enable and trigger DAGs in order:

    fct_event_stream -> dim_users / fct_events -> agg_user_retention, agg_active_user, agg_user_lifecycle_weekly



## Data Models Deep Dive
### Fact Tables
***fct_event_stream*** - Raw Events

- Purpose: Staging layer for raw CSV ingestion
- Validation: 15+ data quality checks on business rules

***fct_events*** - Core Event Stream

- Granularity: One row per user event
- Key Features: JSON event storage, date partitioning, surrogate keys
- Event Types: miles_earned, miles_redeemed, share, like, reward_search

### Dimension Tables
***dim_users*** - User Attributes

- Granularity: One row per user per snapshot date
- Key Metrics: Miles balance, recency flags, latest event JSON
- SCD Type: Type 2 with daily snapshots

### Aggregate Tables
***agg_user_retention*** - Cohort Analysis

- Purpose: Powers retention triangle analysis
- Granularity: cohort_week + week_offset
- Business Value: Identifies which user cohorts have strongest retention

***agg_user_lifecycle_weekly*** - Growth Accounting

- Purpose: Tracks user status changes over time
- Methodology: 4-week lookback window for status classification
- Business Value: Enables user lifecycle marketing campaigns

***agg_active_user*** - Activity Metrics

- Purpose: Multi-granularity active user counts
- Dimensions: time period + acquisition source + country
- Business Value: Measures platform engagement and geographic performance

## Data Quality & Testing Strategy
### Multi-Layer Validation
- Stage 1: Raw Ingestion (Airflow)

    Format validation (timestamp patterns, ID formats)
    Business rule validation (required fields per event type)
    Accepted value validation (enum constraints)

- Stage 2: Transformation (dbt)

    Schema tests (uniqueness, not-null, relationships)
    Custom business logic tests (miles balance calculations)
    Data freshness and volume anomaly detection

- Stage 3: Analytics (dbt)

    Metric consistency tests (retention rates 0-100%)
    Cross-model validation (user counts match across tables)
    Granularity tests (no duplicate primary keys)


## Future Improvements

### Production-Ready Event Storage
Currently, fct_event_stream serves as a historical data dump without partitioning. In production environments, this table should implement proper partitioning strategy (daily/weekly) and incremental loading patterns to handle high-volume event streams efficiently.

### Enhanced Metrics Validation
Implement specialized data quality checks for the three core metrics tables (agg_user_retention, agg_user_lifecycle_weekly, agg_active_user) including business logic validation, trend anomaly detection, and cross-metric consistency checks to ensure analytical accuracy.