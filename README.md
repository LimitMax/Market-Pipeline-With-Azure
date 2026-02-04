# Market Data Pipeline on Azure (Crypto & Stocks)

## Overview
An **end-to-end, cloud-native data engineering pipeline** that ingests market data, enforces data quality, and produces **analytics-ready hourly datasets** stored in **Azure Data Lake Storage Gen2**.

This project was built as a **realistic industry-style case study**, focusing on reliability, idempotency, and cost-aware cloud design.

---

## Case Study

### Problem
Free market data sources (e.g. Yahoo Finance) are easy to access but difficult to use reliably for analytics because they often:
- Contain missing hourly intervals
- Produce inconsistent schemas
- Break during retries or reprocessing
- Risk duplicate or corrupted data during backfills

For analytical and reporting use cases, these issues must be handled systematically—not manually.

---

## Solution

## Architecture Overview
### High-Level Flow
The pipeline is designed as a **layered system**, where each stage has a single responsibility and enforces a clear data contract.

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
### Deployment Validation
- Successfully validated on Azure VM using System-Assigned Managed Identity
- Data written to ADLS Gen2 without any storage keys

---
### Key Design Decisions
- **Contract-driven processing** to prevent silent data corruption  
- **Hourly normalization** with explicit gap detection (`data_gap_flag`)  
- **Idempotent storage strategy** (safe retries & backfills)  
- **Cloud-native but portable design** using filesystem abstraction  
- **Unit-tested core logic** to protect critical transformations  

---

## Results

### Data Quality Guarantees
- Exactly **24 hourly records per asset per day**
- No duplicate `(asset, hour)` keys
- Missing hours are **explicitly flagged**, never hidden

### Operational Outcomes
- Safe historical backfills (Jan 2025 → present)
- Failures isolated per execution date
- Observable pipeline execution via structured logs

### Cost Efficiency
- Azure Data Lake Storage Gen2 (Standard, LRS)
- Estimated cost: **< $1/month**
- Fully compatible with **Azure Student subscription**

---

## Key Learnings
- Enforcing data contracts early prevents downstream analytics issues
- Idempotent design is essential for reliable backfills
- Cloud filesystem abstraction simplifies multi-environment deployments
- Cost-aware architecture does not have to compromise engineering quality

## Impact
This project demonstrates the ability to:
- Design and implement production-style data pipelines
- Apply industry best practices in data quality and reliability
- Deploy cloud-native solutions with cost constraints in mind
- Debug and resolve real-world cloud integration challenges

# Future Improvements
- Managed Identity authentication
- Additional data sources (news, fundamentals)
- Incremental ingestion strategy
- BI semantic layer for reporting

## Tech Stack
- **Language**: Python 3.10  
- **Data Processing**: pandas  
- **Storage Format**: Parquet  
- **Cloud Storage**: Azure Data Lake Storage Gen2  
- **Filesystem Abstraction**: fsspec + adlfs  
- **Testing**: pytest  
- **CI/CD**: GitHub Actions  

---

## How to Run (Local)
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

export PYTHONPATH=src
python src/main.py
