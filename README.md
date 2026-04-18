# iPhone Resale Market Intelligence — Data Pipeline & QA Framework

![CI/CD](https://github.com/kranthi09/iphone-resale-project/actions/workflows/pipeline_tests.yml/badge.svg)
![Python](https://img.shields.io/badge/Python-3.12-blue)
![PySpark](https://img.shields.io/badge/PySpark-3.x-orange)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue)
![Tests](https://img.shields.io/badge/Tests-11%20passing-brightgreen)

An end-to-end data engineering project analysing 2,371 iPhone resale listings across the USA. Includes a PySpark ETL pipeline, SQL analytical layer, automated data quality testing with pytest, and CI/CD integration via GitHub Actions.

---

## Project Overview

This project ingests raw eCommerce resale data, transforms and enriches it using PySpark, loads it into a structured format for analysis, and validates the entire pipeline with an automated test suite that runs on every code push.

**Dataset:** 2,371 iPhone resale listings (eBay USA, 2026)  
**Models covered:** iPhone 12 through iPhone 17  
**Fields:** model, price, condition, seller, US state, storage, discount %, sold units

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Data processing | PySpark (local mode) |
| Database | PostgreSQL 15 |
| Testing | pytest |
| CI/CD | GitHub Actions |
| IDE | VS Code |
| Output | Pandas CSV / Power BI |

---

## Project Structure

```
iphone-resale-project/
├── src/
│   ├── extract.py          # Read raw CSV via Spark
│   ├── transform.py        # Clean, enrich, derive columns
│   ├── load.py             # Write cleaned data to output
│   └── pipeline.py         # Orchestrate full ETL run
├── tests/
│   └── test_pipeline.py    # 11 pytest data quality tests
├── phase2_sql/
│   ├── 01_create_and_import.sql   # Table creation + import
│   ├── 02_cleaned_view.sql        # Cleaned PostgreSQL view
│   ├── 03_analytical_views.sql    # 6 views for Power BI
│   └── 04_data_quality_checks.sql # SQL assertion checks
├── data/
│   └── ecommerce_iphone_resale_market_intelligence_usa_2026.csv
├── .github/
│   └── workflows/
│       └── pipeline_tests.yml  # GitHub Actions CI/CD
└── README.md
```

---

## ETL Pipeline

```
Raw CSV (2,371 rows, 16 cols)
        ↓ extract.py
Spark DataFrame
        ↓ transform.py
Cleaned DataFrame (19 cols)
  - Fixed model_family typo (iPhone 12Pro Max → iPhone 12 Pro Max)
  - Filled nulls (sold, available, discount → 0)
  - Added condition_group (7 conditions → 4 groups)
  - Added price_per_gb derived column
  - Added is_discounted flag
        ↓ load.py
Cleaned CSV output (2,371 rows, 19 cols)
```

### Run the full pipeline

```bash
# Install dependencies
pip install pyspark pandas pytest

# Run full ETL
python src/pipeline.py
```

---

## Data Quality Tests

11 automated tests covering the full pipeline:

```bash
pytest tests/test_pipeline.py -v
```

| Test | What it validates |
|------|------------------|
| `test_row_count` | Raw data has exactly 2,371 rows |
| `test_column_count` | Raw data has 16 columns |
| `test_no_null_prices` | No missing prices in raw data |
| `test_transform_adds_columns` | 3 derived columns added after transform |
| `test_model_typo_fixed` | `iPhone 12Pro Max` typo corrected |
| `test_no_null_condition_group` | All rows have a valid condition group |
| `test_price_range` | All prices between $50 and $6,000 |
| `test_condition_groups_valid` | Only 4 expected condition groups exist |
| `test_output_file_exists` | Output CSV written to disk |
| `test_output_row_count` | Output has 2,371 rows |
| `test_output_column_count` | Output has 19 columns |

---

## CI/CD

Every push to `main` triggers a GitHub Actions workflow that:

1. Sets up Python 3.12 and Java 17 on Ubuntu
2. Installs PySpark, pandas, and pytest
3. Runs all 11 tests automatically

**Workflow file:** `.github/workflows/pipeline_tests.yml`

---

## SQL Analytical Layer (PostgreSQL)

Six analytical views built on top of the cleaned data, ready for Power BI:

| View | Purpose |
|------|---------|
| `vw_price_by_model` | Avg, min, max, median price per model |
| `vw_listings_by_state` | Listing count and avg price by US state |
| `vw_condition_breakdown` | Condition distribution with % of total |
| `vw_storage_vs_price` | Price per GB analysis by model |
| `vw_top_sellers` | Top 20 sellers by units sold |
| `vw_kpi_summary` | Single-row KPI card for dashboards |

---

## Key Findings

- **Most listed model:** iPhone 14 Pro Max
- **Price range:** $65 — $5,000
- **Condition split:** 47% Used, 38% New/Open Box, 10% Refurbished, 5% For Parts
- **Top states:** CA, FL, TX, NY
- **94% of listings** have no "was price" — discount data is sparse

---

## Author

**Kranthi Kumar Yedla**  
MSc Data Analytics — National College of Ireland  
[LinkedIn](https://linkedin.com/in/kranthi-kumar-yedla) | Dublin, Ireland