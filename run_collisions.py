"""
SparkLens — Run Script: NYC Motor Vehicle Collisions Dataset
============================================================
Dataset : Motor Vehicle Collisions – Crashes (NYC OpenData h9gi-nx95)
          https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95
Size    : ~500 MB / 2M+ rows, 29 columns (full dataset)
Platform: Google Dataproc / Apache Spark

Usage:
    python run_collisions.py [--input collisions.csv] [--output ./output]
"""

import argparse
import json
import logging
import os
import sys

# ── Logging setup ────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("SparkLens.Main")

# ── Spark setup ──────────────────────────────────────────────────────
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType

sys.path.insert(0, os.path.dirname(__file__))
from sparklens import SparkLensPipeline


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("SparkLens-NYC-Collisions")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


# ── Pipeline configuration ───────────────────────────────────────────
PIPELINE_CONFIG = {
    # Null normalization
    "null_representations": [
        "N/A", "null", "None", "", "Unknown", "n/a", "NA", "NaN",
        "UNKNOWN", "Unspecified",
    ],

    # Drop columns where > 80% of values are null
    "drop_col_threshold": 0.80,

    # Imputation strategies per column
    # Unspecified columns use default: median for numeric, mode for string
    "imputation_strategies": {
        "borough": "mode",
        "zip_code": "mode",
        "latitude": "median",
        "longitude": "median",
        "on_street_name": "mode",
        "contributing_factor_vehicle_1": "mode",
        "vehicle_type_code1": "mode",
        "number_of_persons_injured": "median",
        "number_of_persons_killed": "constant",
    },
    "fill_values": {
        "number_of_persons_killed": 0,
    },

    # Deduplicate on these columns (collision_id is the natural key)
    "dedup_subset_cols": ["collision_id"],

    # Outlier detection on numeric injury/kill count columns
    "outlier_cols": [
        "number_of_persons_injured",
        "number_of_persons_killed",
        "number_of_pedestrians_injured",
        "number_of_cyclist_injured",
        "number_of_motorist_injured",
        "latitude",
        "longitude",
    ],
    "outlier_method": "iqr",
    "outlier_action": "flag",      # adds <col>_is_outlier boolean columns
    "iqr_multiplier": 1.5,

    # Expected schema for validation
    "expected_schema": {
        "columns": {
            "crash_date": "StringType",
            "crash_time": "StringType",
            "borough": "StringType",
            "zip_code": "StringType",
            "latitude": "DoubleType",
            "longitude": "DoubleType",
            "collision_id": "LongType",
            "number_of_persons_injured": "LongType",
            "number_of_persons_killed": "LongType",
            "contributing_factor_vehicle_1": "StringType",
            "vehicle_type_code1": "StringType",
        },
        "nullable": {
            "collision_id": False,
            "crash_date": False,
        },
    },
}


def cast_schema(df):
    """Cast columns to their proper types after reading as CSV strings."""
    numeric_int_cols = [
        "number_of_persons_injured", "number_of_persons_killed",
        "number_of_pedestrians_injured", "number_of_pedestrians_killed",
        "number_of_cyclist_injured", "number_of_cyclist_killed",
        "number_of_motorist_injured", "number_of_motorist_killed",
    ]
    numeric_double_cols = ["latitude", "longitude"]
    long_cols = ["collision_id"]

    for c in numeric_int_cols:
        if c in df.columns:
            df = df.withColumn(c, F.col(c).cast(LongType()))
    for c in numeric_double_cols:
        if c in df.columns:
            df = df.withColumn(c, F.col(c).cast(DoubleType()))
    for c in long_cols:
        if c in df.columns:
            df = df.withColumn(c, F.col(c).cast(LongType()))
    return df


def main():
    parser = argparse.ArgumentParser(description="SparkLens NYC Collisions Pipeline")
    parser.add_argument("--input",  default="collisions.csv", help="Input CSV path")
    parser.add_argument("--output", default="./output",       help="Output directory (local)")
    args = parser.parse_args()

    # Always resolve to absolute local path
    output_dir = os.path.abspath(args.output)
    os.makedirs(output_dir, exist_ok=True)

    PIPELINE_CONFIG["pre_profile_output"]  = os.path.join(output_dir, "pre_profile.json")
    PIPELINE_CONFIG["post_profile_output"] = os.path.join(output_dir, "post_profile.json")

    # ── Load data ────────────────────────────────────────────────────
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    logger.info(f"Loading dataset: {args.input}")
    df_raw = spark.read.csv(args.input, header=True, inferSchema=False)
    df_raw = cast_schema(df_raw)
    logger.info(f"Raw dataset: {df_raw.count():,} rows × {len(df_raw.columns)} columns")

    # ── Run pipeline ─────────────────────────────────────────────────
    pipeline = SparkLensPipeline(config=PIPELINE_CONFIG, spark=spark)
    df_clean = pipeline.run(df_raw)

    # ── Print summary ────────────────────────────────────────────────
    pipeline.print_summary()

    # ── Save full report ─────────────────────────────────────────────
    report_path = os.path.join(output_dir, "sparklens_report.json")
    with open(report_path, "w") as f:
        json.dump(pipeline.get_full_report(), f, indent=2, default=str)
    logger.info(f"Full report saved: {report_path}")

    # ── Save cleaned data (use pandas to avoid HDFS write issues) ────
    clean_path = os.path.join(output_dir, "collisions_clean.csv")
    df_clean.toPandas().to_csv(clean_path, index=False)
    logger.info(f"Cleaned data saved: {clean_path}")

    # ── Domain analytics on clean data ───────────────────────────────
    logger.info("Running domain analytics on cleaned dataset...")
    _run_analytics(df_clean, output_dir)

    spark.stop()
    logger.info("Done.")


def _run_analytics(df, output_dir: str):
    """Run domain-level analytics and save results."""
    results = {}

    # 1. Collision count by borough
    borough_counts = (
        df.groupBy("borough")
        .count()
        .orderBy(F.col("count").desc())
        .collect()
    )
    results["collisions_by_borough"] = [
        {"borough": r["borough"], "count": r["count"]} for r in borough_counts
    ]

    # 2. Top contributing factors
    top_factors = (
        df.filter(F.col("contributing_factor_vehicle_1").isNotNull())
        .groupBy("contributing_factor_vehicle_1")
        .count()
        .orderBy(F.col("count").desc())
        .limit(10)
        .collect()
    )
    results["top_contributing_factors"] = [
        {"factor": r["contributing_factor_vehicle_1"], "count": r["count"]}
        for r in top_factors
    ]

    # 3. Injury severity summary
    agg_row = df.select(
        F.sum("number_of_persons_injured").alias("total_injured"),
        F.sum("number_of_persons_killed").alias("total_killed"),
        F.avg("number_of_persons_injured").alias("avg_injured_per_crash"),
    ).collect()[0]
    results["injury_severity"] = {
        "total_injured": int(agg_row["total_injured"] or 0),
        "total_killed": int(agg_row["total_killed"] or 0),
        "avg_injured_per_crash": round(float(agg_row["avg_injured_per_crash"] or 0), 4),
    }

    # 4. Crash time distribution (hour)
    hour_dist = (
        df.withColumn("hour", F.split(F.col("crash_time"), ":")[0].cast("int"))
        .groupBy("hour")
        .count()
        .orderBy("hour")
        .collect()
    )
    results["crashes_by_hour"] = [
        {"hour": r["hour"], "count": r["count"]} for r in hour_dist
    ]

    # 5. Vehicle type distribution
    vtype_dist = (
        df.filter(F.col("vehicle_type_code1").isNotNull())
        .groupBy("vehicle_type_code1")
        .count()
        .orderBy(F.col("count").desc())
        .limit(10)
        .collect()
    )
    results["top_vehicle_types"] = [
        {"vehicle_type": r["vehicle_type_code1"], "count": r["count"]}
        for r in vtype_dist
    ]

    analytics_path = os.path.join(output_dir, "analytics_results.json")
    with open(analytics_path, "w") as f:
        import json
        json.dump(results, f, indent=2, default=str)
    logger.info(f"Analytics results saved: {analytics_path}")

    # Print key results
    print("\n" + "═" * 55)
    print("  DOMAIN ANALYTICS RESULTS")
    print("═" * 55)
    print(f"  Total injured : {results['injury_severity']['total_injured']:,}")
    print(f"  Total killed  : {results['injury_severity']['total_killed']:,}")
    print(f"  Avg injured/crash: {results['injury_severity']['avg_injured_per_crash']:.3f}")
    print(f"\n  Top Boroughs:")
    for b in results["collisions_by_borough"][:5]:
        print(f"    {str(b['borough']):20s}  {b['count']:,}")
    print(f"\n  Top Contributing Factors:")
    for fac in results["top_contributing_factors"][:5]:
        print(f"    {str(fac['factor'])[:40]:40s}  {fac['count']:,}")
    print("═" * 55 + "\n")


if __name__ == "__main__":
    main()
