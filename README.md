# HeyMax Loyalty Program Analytics Platform

End-to-end data engineering solution for HeyMax loyalty program analytics. Processes raw CSV event data through a multi-layered pipeline to generate three business analytical dashboards. 


## Technical Architecture
![image](https://drive.google.com/uc?export=view&id=1SpB1suhX2RNIbtk8fzECRrXWOQafYgZJ)


## Data Flow
### Three Layer Data Pipeline
![image](https://drive.google.com/uc?export=view&id=1ryoZuRRCOQf3Arp5BOPsCZxZVFcn4erG)


## Key Design Principles

### Data Modeling & Transformation Logic
  - Modular Pipeline Architecture: Independent layers (raw ingestion → core tables → analytics aggregates) enable isolated testing and deployment
  - Consistent Time Granularity: All models use Sunday-starting weeks for uniform time-based analysis across retention cohorts and lifecycle tracking
  - JSON Flexibility: Event storage in JSON format accommodates schema evolution while maintaining structured analytics

### Dashboard Design & Metric Clarity
  - Business-Focused Metrics: Three dashboards directly address core business questions (retention rates, user lifecycle status, engagement trends)
  - Multi-Dimensional Analysis: Consistent segmentation by acquisition source and geography across all metrics
  - Actionable Insights: Retention triangle enables cohort comparison, lifecycle tracking identifies user segments for targeted campaigns

### Code Readability, Structure & Documentation
  - Comprehensive Data Quality: 15+ validation checks with clear business rule documentation and automatic failure handling
  - Self-Documenting Models: Each dbt model includes detailed purpose, granularity, and business logic explanations in header comments
  - Centralized Configuration: Environment-based database connections, reusable macros, and standardized testing patterns


## Data Model Deep Dive

### Raw Layer

#### fct_event_stream - Clean & validated event data from CSV ingestion
  - Granularity: One row per user event
  - Quality Assurance: 15+ data quality validations (format, enums, required fields)

### Core Data Layer

#### fct_events - Structured event data with enhanced attributes and JSON flexibility
  - Granularity: event_id (surrogate key from event_time + user_id + event_type)
  - Key Features: Date partitioning, JSON formatted event data

#### dim_users - Daily snapshot of user master data
  - Granularity: user_id + ds (daily snapshot)
  - Key Metrics: Miles balance, active flags (7/30/90 days)
  - Key Features: Date partitioning, JSON formatted latest event

### Analytics Layer - Growth Accounting

#### agg_active_user - DAU / WAU / MAU
  - Granularity: period_type(daily / weekly / monthly) + period_start_date + acq_source + country
  - Dimensional Analysis: Segmented by acquisition channel and geographic market

#### agg_user_lifecycle_weekly - Weekly user lifecycle status tracking (New, Retained, Resurrected, Dormant, Churned)
  - Granularity: cutoff_week + acq_source + country + user_status
  - Business value: Answering the critical question, "By week x, how many New/Retained/Resurrected/Dormant/Churned users do we have?"

#### agg_user_retention - Cohort Analysis
  - Granularity: cohort_week + week_offset (0-16 weeks)
  - Business Value: Identifies which user acquisition cohorts demonstrate strongest long-term engagement


## Business Intelligence Dashboards

### Active User Metrics
  - Powered by agg_active_user, monitoring platform engagement metrics: DAU & WAU, with capability of segmenting by acquisition source and user country.
  - Provides operational visibility into user engagement and platform health.
![image](https://drive.google.com/uc?export=view&id=10F6CnCbAB_t-BrObWYtReBT3tugXtJd-)
![image](https://drive.google.com/uc?export=view&id=1CEXXAzo1X1pL4X_o2BpFVuNRc0xUZ6Nq)

### User Lifecycle Management
  - Powered by agg_user_lifecycle_weekly, tracking the lifecycle of users on weekly basis. 
  - Enables identification of user segments for targeted retention campaigns.
![image](https://drive.google.com/uc?export=view&id=1hWmnUPkYgKCvvnL8XEfkKHVg13sSBMbS)

### Cohort Retention Analysis
  - Powered by agg_user_retention, visualizing retention metrics up to 16 weeks. 
  - Critical for understanding long-term user value and acquisition quality.
![image](https://drive.google.com/uc?export=view&id=1Hb1XvycSQeoi_8WbuuxmMSQJhbMQAEJS)


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


## Future Improvements

### Potential bug in miles tracking
24 users are found with negative miles balance (redeemed more miles than actually earned). Needs to work with PROD team to triage the situation.
![image](https://drive.google.com/uc?export=view&id=1xI_JhmfRLfbOsi192DMdcwRK9KyGOC9w)

### Production-Ready Event Storage
Currently, fct_event_stream serves as a historical data dump without partitioning. In production environments, this table should implement proper partitioning strategy (daily/weekly) and incremental loading patterns to handle high-volume event streams efficiently.

### Enhanced Metrics Validation
Implement specialized data quality checks for the three core metrics tables (agg_user_retention, agg_user_lifecycle_weekly, agg_active_user) including business logic validation, trend anomaly detection, and cross-metric consistency checks to ensure analytical accuracy.