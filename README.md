# SparkLens

**A Reusable Spark-Based Library for Data Cleaning and Quality Assessment**

> Team: Harshita Singh (hs5412) · Utkarsh Arora (ua2152) · Debdeep Naha (dn2491)
> Big Data Application Development, Spring 2026

---

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
│   ├── pre_profile.json         # Column profile before cleaning
│   ├── post_profile.json        # Column profile after cleaning
│   ├── sparklens_report.json    # Full pipeline report
│   ├── analytics_results.json   # Domain analytics results
│   └── collisions_clean.csv     # Cleaned dataset
└── README.md
```

---

## Pipeline Modules

| Module | File | Author |
|--------|------|--------|
| Missing Values & Duplicates | `missing_duplicates.py` | Debdeep Naha |
| Outlier Detection | `outlier_detection.py` | Harshita Singh |
| Schema Validation & Profiling | `schema_profiler.py` | Utkarsh Arora |
| Unified Pipeline | `pipeline.py` | Team |

---

## Dataset

- **Name**: Motor Vehicle Collisions — Crashes
- **Source**: NYC OpenData (`h9gi-nx95`)
- **URL**: https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95
- **Size**: ~500 MB / 2M+ rows, 29 columns (full dataset); 5,100-row sample used for demo

---

## Option 1 — Run Locally

### Prerequisites

```bash
pip install pyspark pandas
```

### Run

```bash
python run_collisions.py --input collisions.csv --output ./output
```

---

## Option 2 — Run on Google Dataproc (NYU HPC)

This is the recommended way to run SparkLens at scale.

### Step 1 — SSH into the Dataproc master node

Open the Google Cloud Console, navigate to **Dataproc → Clusters**, click your cluster, and click **SSH** on the master node. Or use the direct SSH-in-browser URL provided by your HPC team.

### Step 2 — Clone the repo on the cluster

```bash
git clone https://github.com/Harshis517/sparklens.git
cd sparklens
```

### Step 3 — Upload the dataset to HDFS

```bash
hadoop fs -mkdir -p /user/$USER/sparklens/input
hadoop fs -put collisions.csv /user/$USER/sparklens/input/
```

Verify the upload:

```bash
hadoop fs -ls /user/$USER/sparklens/input/
```

### Step 4 — Submit the job with spark-submit

```bash
spark-submit \
  --master yarn \
  --deploy-mode client \
  --py-files sparklens/ \
  run_collisions.py \
  --input /user/$USER/sparklens/input/collisions.csv \
  --output ~/sparklens/output
```

> **Note:** `--output` must be a **local path** (e.g. `~/sparklens/output`), not an HDFS path.
> The pipeline writes JSON reports and the cleaned CSV directly to the local filesystem on the master node.

### Step 5 — Check the output

```bash
ls ~/sparklens/output/
cat ~/sparklens/output/analytics_results.json
cat ~/sparklens/output/sparklens_report.json
```

### Step 6 — Download results to your laptop

Click **DOWNLOAD FILE** in the top-right of the SSH-in-browser window and select any file from `~/sparklens/output/`.

Or zip everything first then download:

```bash
cd ~
zip -r sparklens_output.zip sparklens/output/
```

Then download `sparklens_output.zip` via the browser SSH download button.

---

## Configuration

All pipeline behavior is controlled by the `PIPELINE_CONFIG` dictionary at the top of `run_collisions.py`. Key options:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `null_representations` | `["N/A", "null", "None", ...]` | Strings treated as null |
| `drop_col_threshold` | `0.80` | Drop columns with > 80% nulls |
| `imputation_strategies` | per-column | `mean`, `median`, `mode`, or `constant` |
| `dedup_subset_cols` | `["collision_id"]` | Columns used for deduplication |
| `outlier_method` | `iqr` | `iqr` or `zscore` |
| `outlier_action` | `flag` | `flag`, `cap`, or `remove` |
| `iqr_multiplier` | `1.5` | Tukey fence multiplier |

---

## Pipeline Results (NYC Collision Dataset)

| Metric | Before | After |
|--------|--------|-------|
| Rows | 5,100 | 5,000 |
| Columns | 29 | 22 |
| Duplicate rows | 100 | 0 |
| Avg completeness | ~74% | 100% |
| **Quality score** | **52.7 / 100** | **100.0 / 100** |

### Top Domain Findings

1. **Driver Inattention/Distraction** is the leading cause of collisions — 40.3% of all crashes
2. **Brooklyn** has the highest collision count (32%), followed by Queens (21.8%)
3. Pedestrian and cyclist injuries are statistically rare but precisely identifiable via IQR outlier flagging

---

## Acknowledgements

- NYC OpenData for the Motor Vehicle Collisions dataset
- NYC Taxi & Limousine Commission for trip record data
- City of Chicago Open Data Portal for Food Inspections data
- NYU HPC team for Dataproc cluster access
- Apache Spark open-source community
