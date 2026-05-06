# SparkLens

**A Reusable Spark-Based Library for Data Cleaning and Quality Assessment**

Team: Utkarsh Arora (ua2152) · Harshita Singh (hs5412) · Debdeep Naha (dn2491)
Big Data Analytics Symposium, Spring 2026

## Project Structure

```
sparklens/
├── sparklens/                   # Library package
│   ├── __init__.py              # Exports all modules
│   ├── missing_duplicates.py    # Module 1: Null normalization, imputation, dedup
│   ├── outlier_detection.py     # Module 2: IQR / Z-score outlier detection
│   ├── schema_profiler.py       # Module 3: Schema validation + column profiling
│   └── pipeline.py              # Unified pipeline orchestrator
├── run_collisions.py            # Main entry point: NYC Collision dataset
├── collisions.csv               # Dataset (NYC Motor Vehicle Collisions - Crashes)
├── output/                      # Pipeline outputs (generated on run)
│   ├── pre_profile.json
│   ├── post_profile.json
│   ├── sparklens_report.json
│   ├── analytics_results.json
│   └── collisions_clean.csv/
└── README.md
```

## Installation

```bash
pip install pyspark
```

## Usage

```bash
python run_collisions.py --input collisions.csv --output ./output
```

## Pipeline Modules

| Module | File | Author |
|--------|------|--------|
| Missing Values & Duplicates | missing_duplicates.py | Debdeep Naha |
| Outlier Detection | outlier_detection.py | Harshita Singh |
| Schema Validation & Profiling | schema_profiler.py | Utkarsh Arora |
| Unified Pipeline | pipeline.py | Team |

## Dataset

- **Name**: Motor Vehicle Collisions — Crashes
- **Source**: NYC OpenData (h9gi-nx95)
- **URL**: https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95
- **Size**: ~500 MB / 2M+ rows, 29 columns (full); 5,100 rows used for demo
