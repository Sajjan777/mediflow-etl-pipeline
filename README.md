# MediFlow — Automated ETL Pipeline for Medicare Claims Data

## Overview
MediFlow is an automated data pipeline built with Apache Airflow and Python that automatically downloads, validates, transforms, and delivers clean U.S. Medicare Part D prescriber data every month — with zero manual steps.

## Problem Statement
Raw CMS healthcare data arrives in an inconsistent format with duplicate records, unstandardized fields, and no derived metrics — making it unreliable for direct analytical use. MediFlow solves this by automating the entire download, validation and transformation process, ensuring consistency and repeatability across every pipeline run.

## Pipeline Architecture
CMS API → [Download] → [Validate] → [Transform] → [Clean CSV]

## Tech Stack
- Apache Airflow 2.8.1 (orchestration)
- Python 3.8 (scripting)
- Pandas (data transformation)
- Requests (API integration)
- Docker (containerization)

## Data Source
CMS Medicare Part D Prescribers by Provider (2024)
https://data.cms.gov/provider-summary-by-type-of-service/medicare-part-d-prescribers/medicare-part-d-prescribers-by-provider

## Pipeline Tasks

### Task 1 — download_cms_data
Automatically fetches latest Medicare Part D data from CMS public API:
- Fetches data in batches of 6,000 rows
- Retries failed requests up to 3 times with exponential backoff
- Writes directly to disk to avoid memory issues
- Stops automatically when all rows are downloaded

### Task 2 — validate_raw_data
Runs 5 data quality checks before transformation:
- Empty file check
- Required column presence check
- Null count logging on critical columns
- Negative value detection
- Duplicate NPI detection

### Task 3 — transform_and_clean_data
Applies 7 transformation steps:
- Column names standardized to lowercase
- Duplicate NPI rows removed
- Null values filled in critical columns
- State abbreviations standardized to uppercase
- Derived metric created: avg_cost_per_claim
- Pipeline metadata added: processed_at, data_source
- Clean output saved to data/processed/

### Task 4 — confirm_load
Reads the processed file and confirms row count and structure.

## Transformation Logic

| Raw Column           | Processed Column       | Rule Applied                  |
|----------------------|------------------------|-------------------------------|
| PRSCRBR_NPI          | prscrbr_npi            | Lowercased, deduped           |
| Prscrbr_State_Abrvtn | prscrbr_state_abrvtn   | Uppercased, nulls → UNKNOWN   |
| Tot_Clms             | tot_clms               | Nulls filled with 0           |
| Tot_Drug_Cst         | tot_drug_cst           | Nulls filled with 0.0         |
| (derived)            | avg_cost_per_claim     | tot_drug_cst / tot_clms       |
| (derived)            | processed_at           | Pipeline run timestamp        |
| (derived)            | data_source            | CMS_Medicare_PartD            |

## Results

| Metric              | Value         |
|---------------------|---------------|
| Total rows          | 1,416,883     |
| Total columns       | 87            |
| Duplicate rows      | 0             |
| Null critical cols  | 0             |
| Pipeline schedule   | Monthly       |

## How to Run
1. Clone the repository
2. Make sure Docker Desktop is running
3. Run: `docker compose up -d`
4. Open: `http://localhost:8080`
5. Login: username `airflow` / password `airflow`
6. Trigger the `healthcare_claims_etl` DAG

## What Makes It Production-Style
- Fully automated — runs monthly with zero manual intervention
- Resilient — retries failed API calls with exponential backoff
- Memory efficient — streams data directly to disk in batches
- Observable — every step logs progress and errors
- Validated — 5 quality checks before every transformation
- Documented — full transformation logic and architecture
- Version controlled — GitHub with meaningful commit history
- Containerized — runs identically on any machine with Docker