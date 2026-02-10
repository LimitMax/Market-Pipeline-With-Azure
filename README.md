# Market Data Pipeline on Azure (Crypto & Stocks)

## Overview
An end-to-end, cloud-native data pipeline designed to support reliable analytics, ingesting market data, enforcing strict data quality rules, and producing analytics-ready hourly datasets stored in Azure Data Lake Storage Gen2.

Built as a realistic industry-style case study, this project combines a Data Analyst’s focus on data correctness with Data Engineering practices such as idempotency, watermarking, and cost-aware cloud design.

This project was developed from a Data Analyst background to solve real analytical reliability issues using production-grade data engineering practices.

---

## Background
Market data is widely used for analytics such as performance analysis, return calculation, and monitoring. However, free market data sources (e.g. Yahoo Finance), while easy to access, are not designed for reliable analytical workloads. From an analytics perspective, common issues include:
- Incomplete hourly data,
- Unstable historical results after refresh,
- Pipelines that fail or corrupt data when re-run.

These problems reduce trust in analytics and consume significant analyst time for manual validation and cleanup.

---
## Problem Statement
The main challenges addressed by this project are:
### 1. Analytical Problems
- Uncertainty about data completeness
- Inconsistent analytical results across runs
- Manual data cleaning before analysis

### 2. Engineering Problems
- Non-idempotent pipelines
- Unsafe retries and backfills
- No clear notion of “last valid data"

---

## Solution Approach

This project implements a production-style market data pipeline with one core principle:
"Ensure analytical correctness through engineering-grade reliability."

Key design decisions include:
### 1). Daily Watermarking
Tracks the last fully processed date per asset, enabling safe retries and automatic catch-up.
### 2). Market-aware Processing
- Crypto: strict 24-hour completeness
- Stocks: trading hours only => Incomplete days are explicitly skipped.
### 3). Contract-driven Validation
Data is validated before entering the data lake; invalid data fails fast.
### 4). Idempotent Storage
Partitioned by asset/date, safe for reprocessing without duplication.
### 5). Clear Execution Modes
Scheduled, manual override, and controlled backfill.

---
## Results
### Data Quality Guarantees
- Crypto assets always produce exactly 24 hourly records per day
- No duplicate (asset, hour) keys
- Invalid prices or volumes fail fast
- Missing data is never silently filled

### Operational Impact
- Pipeline can be run anytime without manual intervention
- Partial failures are isolated per asset
- Pipeline state is transparent via watermarks

### Analytical Impact
- Analytics-ready datasets
- Reproducible and trustworthy results
- Reduced manual data validation effort

---
## Architecture Overview
The pipeline follows a layered, contract-driven architecture, designed with Data Engineer rigor and Data Analyst requirements.

    +------------------+
    |  Data Source     |
    |  (yfinance API)  |
    +---------+--------+
              |
              v
    +------------------+
    | Ingestion Layer  |
    | (Retry + Errors) |
    +---------+--------+
              |
              v
    +------------------+
    | Validation Layer |
    | (Schema & Rules) |
    +---------+--------+
              |
              v
    +------------------+
    | Cleaning Layer   |
    | (Standardization|
    |  & Type Safety) |
    +---------+--------+
              |
              v
    +------------------+
    | Normalization    |
    | (Hourly Grid,   |
    | Gap Detection)  |
    +---------+--------+
              |
              v
    +---------------------------+
    | Storage Layer             |
    | ADLS Gen2 (Parquet)       |
    | Idempotent Partitions     |
    +---------------------------+

---

## Tech Stack
- **Language**: Python 3.10  
- **Data Processing**: pandas  
- **Storage Format**: Parquet  
- **Cloud Storage**: Azure Data Lake Storage Gen2  
- **Auth**: Managed Identity 
- **Logging**: system logs + progress logs 
- **Testing**: pytest  
- **CI/CD**: GitHub Actions  
---
## Future Improvements
- Hour-level watermarking for near real-time crypto data
- SLA and lateness policy enforcement
- Market calendar awareness for stock data
- Monitoring and alerting
- Semantic layer for BI consumption

---

## Closing Note
This project reflects a hybrid approach to data work:
**Thinking as a Data Analyst, executing as a Data Engineer, building pipelines that protect analytical trust.**

## How to Run (Local)
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

local run schedule:
export ENV=local
export STORAGE_BASE_PATH="xxx"
export SHOW_PROGRESS=true
export EXECUTION_DATE=2025-02-24 => adjust date
export PYTHONPATH=src
python src/main.py

run cloud/production schedule via VM:
export ENV=cloud
export STORAGE_BASE_PATH="xxxx"
export SHOW_PROGRESS=true
export EXECUTION_DATE=2025-02-24 => adjust date
export PYTHONPATH=src
python src/main.py or python src/pipeline/backfill.py
