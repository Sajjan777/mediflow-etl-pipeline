# MediFlow — Automated ETL Pipeline for Medicare Claims Data

## Overview
MediFlow is an automated data pipeline built with Apache Airflow and Python 
that ingests raw U.S. Medicare Part D prescriber data, validates its quality, 
transforms it into a standardized schema, and delivers a clean, 
analysis-ready dataset.

## Problem Statement
Raw CMS healthcare data arrives in an inconsistent format with duplicate 
records, unstandardized fields, and no derived metrics — making it 
unreliable for direct analytical use. MediFlow solves this by automating 
the entire validation and transformation process, ensuring consistency 
and repeatability across every pipeline run.

## Pipeline Architecture

## Tech Stack
- Apache Airflow 2.8.1 (orchestration)
- Python 3.8 (scripting)
- Pandas (data transformation)
- Docker (containerization)

## Data Source
CMS Medicare Part D Prescribers by Provider (2023)
https://data.cms.gov/provider-summary-by-type-of-service/medicare-part-d-prescribers/medicare-part-d-prescribers-by-provider

## Pipeline Tasks

### Task 1 — validate_raw_data
Runs 5 data quality checks on the raw file:
- Empty file check
- Required column presence check
- Null count logging on critical columns
- Negative value detection
- Duplicate NPI detection

### Task 2 — transform_and_clean_data
Applies 7 transformation steps:
- Column names standardized to lowercase
- Duplicate NPI rows removed
- Null values filled in critical columns
- State abbreviations standardized to uppercase
- Derived metric created: avg_cost_per_claim
- Pipeline metadata added: processed_at, data_source
- Clean output saved to data/processed/

### Task 3 — confirm_load
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
| Raw file size       | 569 MB        |
| Clean file size     | 222 MB        |
| Total rows          | 1,380,665     |
| Total columns       | 87            |
| Duplicate rows      | 0             |
| Null critical cols  | 0             |

## How to Run
1. Clone the repository
2. Make sure Docker Desktop is running
3. Run: `docker compose up -d`
4. Open: `http://localhost:8080`
5. Login: username `airflow` / password `airflow`
6. Trigger the `healthcare_claims_etl` DAG